"""Germany (DE-LU) market adapter.

Market model in brief (sources and limits are documented in ``docs/germany.md``):

* Day-ahead: EPEX/SDAC, DE-LU bidding zone, local time Europe/Berlin. The SDAC market time unit is
  15 minutes from delivery day 2025-10-01 and one hour before. Hourly rows are expanded to four flat
  quarter-hours tagged ``expanded_from_60m`` so the 15-minute engine grid never invents intra-hour shape.
* No implicit balancing: balance responsible parties must keep their balance group balanced in every
  quarter-hour (Bilanzkreistreue). ``da_plus_imbalance`` is therefore rejected with an explicit error.
  The imbalance price (reBAP) is documented but never traded.
* FCR: symmetric 4-hour products, daily auction closing D-1 08:00, pay-as-cleared (FCR Cooperation via
  regelleistung.net). Limited-energy reservoirs must keep an energy buffer in both directions.
* aFRR capacity: asymmetric 4-hour products (POS/NEG), daily auction closing D-1 09:00, pay-as-bid. The
  benchmark price is the published German average awarded capacity price and is labelled as such.
  aFRR energy (PICASSO) is not modelled: the German aFRR path is capacity-only with zero activation.

The engine path (``run_walk_forward``) reuses the framework solver, which lets reserve volumes vary per
quarter-hour. German products are 4-hour blocks, so :func:`solve_germany_day` provides a block-constant
co-optimisation used by ``scripts/research/germany_backtest.py`` for the headline numbers.
"""

from __future__ import annotations

import json
import re
import zipfile
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from datetime import date, timedelta
from pathlib import Path
from typing import Any, Literal
from xml.etree import ElementTree as ET

import numpy as np
import pandas as pd

from ...config import GERMANY_IMBALANCE_REJECTION_MESSAGE, BacktestConfig
from ...reserve import (
    AfrrAsymmetricReserveProduct,
    CapacityOnlyReserveSettlement,
    CapacityPlusActivationReserveSettlement,
    ExpectedActivationAssumption,
    NoActivationAssumption,
    SymmetricCapacityReserveProduct,
)
from ...types import PriceSeries
from .base import LoadedMarketData, MarketAdapter, NoImbalanceSettlement, SettlementRule

GERMANY_MARKET_ID = "germany"
GERMANY_TIMEZONE = "Europe/Berlin"
DE_LU_BIDDING_ZONE = "10Y1001A1001A82H"
GERMANY_LFC_AREA = "10Y1001A1001A83F"
DAY_AHEAD_QUARTER_HOUR_MTU_START = date(2025, 10, 1)
DAY_AHEAD_GATE_CLOSURE_LOCAL = "12:00"
FCR_GATE_CLOSURE_LOCAL = "08:00"
AFRR_CAPACITY_GATE_CLOSURE_LOCAL = "09:00"
RESERVE_PRODUCT_BLOCK_HOURS = 4
RESERVE_PRODUCT_BLOCK_MINUTES = RESERVE_PRODUCT_BLOCK_HOURS * 60
# PQ-Bedingungen fuer FCR, aFRR und mFRR in Deutschland (v1.04, 2022), section 3.1.6: a 1 MW symmetric FCR
# unit with a limited energy reservoir needs 0.83 MWh in total, i.e. 0.415 MWh per direction per MW
# (alert state 0.25 + max(previous activation, management lag) 0.125 + reserve-mode bridge 0.04).
FCR_LER_ENERGY_HOURS_PER_DIRECTION = 0.415
# Same rule if the CE minimum activation period (SOGL Art. 156(10), 15-30 min) were set to 30 minutes.
FCR_LER_ENERGY_HOURS_30_MIN_ALERT_STATE = 0.665
# Engine path: FcrConfig.sustain_duration_minutes must be a multiple of 15; 30 >= 24.9 minutes.
FCR_ENGINE_SUSTAIN_MINUTES = 30
# PQ-Bedingungen v1.04, section 2.7.1: FRR units with limited reservoirs need 60 minutes per marketed MW.
AFRR_LER_ENERGY_HOURS_PER_DIRECTION = 1.0
AFRR_ENGINE_SUSTAIN_MINUTES = 60
ENERGY_CHARTS_ATTRIBUTION = "Bundesnetzagentur | SMARD.de via Energy-Charts (Fraunhofer ISE), CC BY 4.0"

_SUPPORTED_SOURCE_RESOLUTIONS = (15, 30, 60, 240)
_RESOLUTION_CODES = {"PT15M": 15, "PT30M": 30, "PT60M": 60, "PT1H": 60, "PT4H": 240}
_PRICE_COLUMNS = ("price_eur_per_mwh", "price_eur_mwh", "price_eur_per_mw_h", "price")
_DIRECTION_ALIASES = {
    "up": "up",
    "pos": "up",
    "positive": "up",
    "down": "down",
    "neg": "down",
    "negative": "down",
    "symmetric": "symmetric",
    "negpos": "symmetric",
    "sym": "symmetric",
}
_XLSX_NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
_XLSX_MAX_MEMBER_BYTES = 25_000_000
_PRODUCT_NAME = re.compile(r"^(NEGPOS|POS|NEG)_(\d{2})_(\d{2})$")


# ---------------------------------------------------------------------------------------------------------------
# Time-series normalisation
# ---------------------------------------------------------------------------------------------------------------


def _price_column(frame: pd.DataFrame) -> str:
    for column in _PRICE_COLUMNS:
        if column in frame.columns:
            return column
    raise ValueError(f"Input series needs one of the price columns {list(_PRICE_COLUMNS)}")


def energy_charts_payload_to_frame(payload: Mapping[str, Any]) -> pd.DataFrame:
    """Convert an Energy-Charts ``/price`` payload (``unix_seconds`` + ``price``) to a UTC frame."""

    if "unix_seconds" not in payload or "price" not in payload:
        raise ValueError("Energy-Charts payload must contain `unix_seconds` and `price`")
    seconds = np.asarray(payload["unix_seconds"], dtype="int64")
    prices = pd.to_numeric(pd.Series(payload["price"], dtype="object"), errors="coerce").astype(float)
    if len(seconds) != len(prices):
        raise ValueError("Energy-Charts payload has mismatched `unix_seconds` and `price` lengths")
    frame = pd.DataFrame(
        {
            "timestamp_utc": pd.to_datetime(seconds, unit="s", utc=True),
            "price_eur_per_mwh": prices.to_numpy(),
        }
    )
    frame["source"] = "energy_charts_api"
    return frame


def infer_source_resolution_minutes(timestamps_utc: pd.Series, *, timezone: str = GERMANY_TIMEZONE) -> pd.Series:
    """Infer the source resolution of every row from the spacing inside its local delivery day.

    The SDAC switched from hourly to 15-minute products between delivery days, so the resolution is
    constant within a local day. Gaps inside a day raise instead of being silently filled.
    """

    stamps = pd.Series(pd.to_datetime(timestamps_utc, utc=True)).reset_index(drop=True)
    if stamps.empty:
        raise ValueError("Cannot infer resolution of an empty series")
    if not stamps.is_monotonic_increasing or stamps.duplicated().any():
        raise ValueError("Timestamps must be unique and sorted before inferring resolution")
    local_day = stamps.dt.tz_convert(timezone).dt.date
    step = (stamps.shift(-1) - stamps).dt.total_seconds().div(60.0)
    same_day = local_day.shift(-1) == local_day
    within = step.where(same_day)
    resolutions = pd.Series(np.nan, index=stamps.index, dtype=float)
    for day, positions in within.groupby(local_day).groups.items():
        values = within.loc[positions].dropna()
        if values.empty:
            raise ValueError(f"Cannot infer the resolution of local day {day}: it contains a single row")
        unique = values.unique()
        if len(unique) != 1:
            raise ValueError(f"Local day {day} mixes resolutions or has gaps: steps {sorted(unique.tolist())} minutes")
        resolution = float(unique[0])
        if int(resolution) not in _SUPPORTED_SOURCE_RESOLUTIONS:
            raise ValueError(f"Local day {day} has unsupported resolution {resolution} minutes")
        resolutions.loc[positions] = resolution
    return resolutions.astype(int)


def _expand_starts(starts_utc: pd.Series, repeats: np.ndarray) -> tuple[np.ndarray, pd.DatetimeIndex]:
    """Repeat every start ``repeats[i]`` times, stepping 15 minutes; returns (source row index, timestamps)."""

    row_index = np.repeat(np.arange(len(starts_utc)), repeats)
    offsets = np.concatenate([np.arange(count) for count in repeats]) * 15 if len(repeats) else np.array([], int)
    naive = pd.DatetimeIndex(pd.to_datetime(starts_utc, utc=True)).tz_convert("UTC").tz_localize(None)
    timestamps = pd.DatetimeIndex(naive.to_numpy()[row_index]).tz_localize("UTC") + pd.to_timedelta(offsets, unit="min")
    return row_index, pd.DatetimeIndex(timestamps)


def _row_resolution(frame: pd.DataFrame, *, timezone: str) -> pd.Series:
    """Interval length of every input row in minutes (``source_resolution_minutes`` is provenance only)."""

    if "resolution_minutes" in frame.columns:
        return pd.to_numeric(frame["resolution_minutes"]).astype(int).reset_index(drop=True)
    if "resolution" in frame.columns:
        mapped = frame["resolution"].astype(str).str.upper().map(_RESOLUTION_CODES)
        if mapped.isna().any():
            unknown = sorted(frame.loc[mapped.isna(), "resolution"].astype(str).unique().tolist())
            raise ValueError(f"Unsupported resolution codes: {unknown}")
        return mapped.astype(int).reset_index(drop=True)
    return infer_source_resolution_minutes(frame["timestamp_utc"], timezone=timezone)


def normalize_quarter_hour_frame(
    frame: pd.DataFrame,
    *,
    market: str,
    zone: str,
    source: str,
    timezone: str = GERMANY_TIMEZONE,
    currency: str = "EUR",
) -> pd.DataFrame:
    """Return a canonical 15-minute frame, expanding coarser rows (e.g. hourly day-ahead) with provenance.

    Accepted inputs carry ``timestamp_utc`` plus one price column (``price_eur_per_mwh``, ``price_eur_mwh``,
    ``price_eur_per_mw_h`` or ``price``). Resolution comes from ``resolution_minutes``/``resolution`` when
    present and is otherwise inferred per local delivery day. Expanded rows keep the source price flat over
    the quarter-hours of the source interval and are tagged ``expanded_from_<N>m``.
    """

    if "timestamp_utc" not in frame.columns:
        raise ValueError("Input series needs a `timestamp_utc` column")
    price_column = _price_column(frame)
    working = frame.copy()
    working["timestamp_utc"] = pd.to_datetime(working["timestamp_utc"], utc=True, format="mixed")
    working = working.sort_values("timestamp_utc").reset_index(drop=True)
    if working["timestamp_utc"].duplicated().any():
        raise ValueError(f"{market} input contains duplicate timestamps")
    resolution = _row_resolution(working, timezone=timezone)
    if ((resolution % 15) != 0).any() or (resolution <= 0).any():
        raise ValueError(f"{market} input resolution must be a positive multiple of 15 minutes")
    prices = pd.to_numeric(working[price_column], errors="coerce").astype(float)
    if prices.isna().any():
        missing = working.loc[prices.isna(), "timestamp_utc"].iloc[0]
        raise ValueError(f"{market} input has missing prices (first at {missing.isoformat()})")
    original_provenance = (
        working["provenance"].astype(str).reset_index(drop=True)
        if "provenance" in working.columns
        else pd.Series("", index=working.index, dtype=str)
    )
    original_source = (
        working["source"].astype(str).reset_index(drop=True)
        if "source" in working.columns
        else pd.Series(source, index=working.index, dtype=str)
    )
    repeats = (resolution // 15).to_numpy()
    row_index, timestamps = _expand_starts(working["timestamp_utc"], repeats)
    row_resolution = resolution.to_numpy()[row_index]
    # A frame that was already normalised keeps its original source resolution (e.g. 60 for expanded hours).
    source_resolution = (
        pd.to_numeric(working["source_resolution_minutes"]).astype(int).to_numpy()[row_index]
        if "source_resolution_minutes" in working.columns
        else row_resolution
    )
    base_provenance = pd.Series(original_provenance.to_numpy()[row_index], dtype=str)
    expanded_tag = "expanded_from_" + pd.Series(row_resolution, dtype=int).astype(str) + "m"
    inherited_tag = "expanded_from_" + pd.Series(source_resolution, dtype=int).astype(str) + "m"
    provenance = np.where(
        row_resolution == 15,
        np.where(
            base_provenance == "", np.where(source_resolution == 15, "source_15m", inherited_tag), base_provenance
        ),
        np.where(base_provenance == "", expanded_tag, base_provenance + ";" + expanded_tag),
    )
    normalized = pd.DataFrame(
        {
            "timestamp_utc": timestamps,
            "timestamp_local": timestamps.tz_convert(timezone),
            "market": market,
            "zone": zone,
            "resolution_minutes": 15,
            "source_resolution_minutes": source_resolution.astype(int),
            "price_eur_per_mwh": prices.to_numpy()[row_index],
            "currency": currency,
            "source": original_source.to_numpy()[row_index],
            "value_kind": "actual",
            "provenance": provenance.astype(str),
        }
    )
    if normalized["timestamp_utc"].duplicated().any():
        raise ValueError(f"{market} input has overlapping intervals after quarter-hour expansion")
    return normalized


def normalize_day_ahead_frame(
    frame: pd.DataFrame,
    *,
    zone: str = DE_LU_BIDDING_ZONE,
    source: str = "configured_input",
    timezone: str = GERMANY_TIMEZONE,
) -> pd.DataFrame:
    """Normalise DE-LU day-ahead prices to the 15-minute grid (hourly MTU rows become 4 flat quarter-hours)."""

    return normalize_quarter_hour_frame(frame, market="day_ahead", zone=zone, source=source, timezone=timezone)


def day_ahead_mtu_mismatch_days(frame: pd.DataFrame, *, timezone: str = GERMANY_TIMEZONE) -> list[str]:
    """Local delivery days whose source resolution contradicts the SDAC MTU (60 min before, 15 min from 2025-10-01)."""

    if "source_resolution_minutes" not in frame.columns:
        return []
    local_day = pd.to_datetime(frame["timestamp_utc"], utc=True).dt.tz_convert(timezone).dt.date
    expected = np.where(local_day >= DAY_AHEAD_QUARTER_HOUR_MTU_START, 15, 60)
    mismatched = sorted({str(day) for day in local_day[frame["source_resolution_minutes"].to_numpy() != expected]})
    return mismatched


def _normalize_direction(value: object) -> str:
    key = str(value).strip().lower()
    if key not in _DIRECTION_ALIASES:
        raise ValueError(f"Unsupported reserve direction `{value}`")
    return _DIRECTION_ALIASES[key]


def expand_reserve_blocks(
    blocks: pd.DataFrame,
    *,
    market: str,
    zone: str = GERMANY_LFC_AREA,
    source: str = "configured_input",
    timezone: str = GERMANY_TIMEZONE,
    direction: str | None = None,
) -> pd.DataFrame:
    """Expand reserve product blocks (``delivery_start_utc``/``delivery_end_utc``) to the 15-minute grid.

    ``price_eur_per_mw_h`` (EUR per MW per hour) is copied to every quarter-hour of the block and stored in the
    framework column ``price_eur_per_mwh`` (which holds EUR/MW/h for capacity series). If ``direction`` is
    given, only blocks of that direction are kept (aliases: up/POS, down/NEG, symmetric/NEGPOS).
    """

    required = {"delivery_start_utc", "delivery_end_utc"}
    missing = required.difference(blocks.columns)
    if missing:
        raise ValueError(f"Reserve block input is missing columns: {sorted(missing)}")
    working = blocks.copy()
    if direction is not None:
        if "direction" not in working.columns:
            raise ValueError("Reserve block input needs a `direction` column to select one direction")
        wanted = _normalize_direction(direction)
        working = working.loc[working["direction"].map(_normalize_direction) == wanted].copy()
        if working.empty:
            raise ValueError(f"Reserve block input has no `{wanted}` rows for {market}")
    price_column = "price_eur_per_mw_h" if "price_eur_per_mw_h" in working.columns else _price_column(working)
    working["delivery_start_utc"] = pd.to_datetime(working["delivery_start_utc"], utc=True, format="mixed")
    working["delivery_end_utc"] = pd.to_datetime(working["delivery_end_utc"], utc=True, format="mixed")
    working = working.sort_values("delivery_start_utc").reset_index(drop=True)
    durations = (working["delivery_end_utc"] - working["delivery_start_utc"]).dt.total_seconds().div(60.0)
    if (durations <= 0).any() or ((durations % 15) != 0).any():
        raise ValueError(f"{market} blocks must have a positive duration that is a multiple of 15 minutes")
    prices = pd.to_numeric(working[price_column], errors="coerce").astype(float)
    if prices.isna().any():
        raise ValueError(f"{market} blocks have missing prices")
    repeats = (durations // 15).astype(int).to_numpy()
    row_index, timestamps = _expand_starts(working["delivery_start_utc"], repeats)
    starts = pd.DatetimeIndex(pd.to_datetime(working["delivery_start_utc"], utc=True))[row_index]
    block_minutes = durations.astype(int).to_numpy()[row_index]
    sources = (
        working["source"].astype(str).to_numpy()[row_index]
        if "source" in working.columns
        else np.full(len(row_index), source, dtype=object)
    )
    block_tag = "expanded_from_" + pd.Series(block_minutes, dtype=int).astype(str) + "m_block"
    if "provenance" in working.columns:
        original = pd.Series(working["provenance"].astype(str).to_numpy()[row_index], dtype=str)
        provenance = np.where(original == "", block_tag, original + ";" + block_tag)
    else:
        provenance = block_tag.to_numpy()
    expanded = pd.DataFrame(
        {
            "timestamp_utc": timestamps,
            "timestamp_local": timestamps.tz_convert(timezone),
            "market": market,
            "zone": zone,
            "resolution_minutes": 15,
            "source_resolution_minutes": block_minutes,
            "price_eur_per_mwh": prices.to_numpy()[row_index],
            "currency": "EUR",
            "source": sources,
            "value_kind": "actual",
            "provenance": provenance,
            "block_start_utc": starts,
        }
    )
    for optional in ("volume_mw", "marginal_price_eur_per_mw_h", "product"):
        if optional in working.columns:
            expanded[optional] = working[optional].to_numpy()[row_index]
    if expanded["timestamp_utc"].duplicated().any():
        raise ValueError(f"{market} blocks overlap")
    return expanded


def reserve_block_index(
    timestamps_utc: pd.Series | pd.DatetimeIndex, *, timezone: str = GERMANY_TIMEZONE
) -> np.ndarray:
    """Index (0-5) of the local 4-hour product block (00-04, ..., 20-24) for every quarter-hour.

    Blocks are defined in local time, so on DST days the first block lasts 3 or 5 hours.
    """

    local = pd.DatetimeIndex(pd.to_datetime(timestamps_utc, utc=True)).tz_convert(timezone)
    return np.asarray(local.hour // RESERVE_PRODUCT_BLOCK_HOURS, dtype=int)


# ---------------------------------------------------------------------------------------------------------------
# regelleistung.net result exports (xlsx, read with the standard library only)
# ---------------------------------------------------------------------------------------------------------------


def _xlsx_column_index(reference: str) -> int:
    match = re.match(r"([A-Z]+)", reference)
    if match is None:
        raise ValueError(f"Invalid cell reference {reference}")
    number = 0
    for character in match.group(1):
        number = number * 26 + (ord(character) - 64)
    return number - 1


def read_xlsx_rows(path: str | Path) -> list[list[str | None]]:
    """Read the first worksheet of a simple xlsx file (shared strings + numbers) without openpyxl."""

    with zipfile.ZipFile(path) as archive:
        names = set(archive.namelist())
        for info in archive.infolist():
            if info.file_size > _XLSX_MAX_MEMBER_BYTES:
                raise ValueError(f"xlsx member {info.filename} is unexpectedly large ({info.file_size} bytes)")
        shared: list[str] = []
        if "xl/sharedStrings.xml" in names:
            root = ET.fromstring(archive.read("xl/sharedStrings.xml"))
            for item in root.findall(f"{_XLSX_NS}si"):
                shared.append("".join(node.text or "" for node in item.iter(f"{_XLSX_NS}t")))
        sheets = sorted(name for name in names if name.startswith("xl/worksheets/sheet") and name.endswith(".xml"))
        if not sheets:
            raise ValueError(f"{path} does not contain a worksheet")
        root = ET.fromstring(archive.read(sheets[0]))
    rows: list[list[str | None]] = []
    for row in root.iter(f"{_XLSX_NS}row"):
        values: dict[int, str | None] = {}
        for cell in row.findall(f"{_XLSX_NS}c"):
            reference = cell.get("r")
            if reference is None:
                continue
            kind = cell.get("t")
            value_node = cell.find(f"{_XLSX_NS}v")
            if kind == "s" and value_node is not None and value_node.text is not None:
                values[_xlsx_column_index(reference)] = shared[int(value_node.text)]
            elif kind == "inlineStr":
                values[_xlsx_column_index(reference)] = "".join(node.text or "" for node in cell.iter(f"{_XLSX_NS}t"))
            else:
                values[_xlsx_column_index(reference)] = value_node.text if value_node is not None else None
        if values:
            rows.append([values.get(position) for position in range(max(values) + 1)])
    return rows


def _rows_to_frame(rows: Sequence[Sequence[str | None]]) -> pd.DataFrame:
    if not rows:
        raise ValueError("Result export is empty")
    header = [str(value) if value is not None else f"column_{index}" for index, value in enumerate(rows[0])]
    body = [list(row) + [None] * (len(header) - len(row)) for row in rows[1:]]
    return pd.DataFrame(body, columns=header)


def _number(raw: object) -> float:
    if raw is None or (isinstance(raw, str) and raw.strip() in {"", "-", "–", "n/a", "N/A"}):
        return float("nan")
    return float(str(raw))


def _deduplicate_products(records: list[dict[str, object]]) -> pd.DataFrame:
    """Drop rows without a German price and keep the last row per product (later tenders supersede earlier ones)."""

    frame = pd.DataFrame(records)
    if frame.empty:
        raise ValueError("Result export does not contain any German product prices")
    frame = frame.loc[frame["price_eur_per_mw_h"].notna()]
    frame = frame.drop_duplicates(["delivery_start_utc", "direction"], keep="last")
    return frame.sort_values(["direction", "delivery_start_utc"]).reset_index(drop=True)


def _excel_or_iso_date(value: object) -> date:
    text = str(value).strip()
    try:
        serial = float(text)
    except ValueError:
        return pd.Timestamp(text).date()
    return date(1899, 12, 30) + timedelta(days=int(serial))


def _product_window_utc(
    delivery_day: date, product_name: str, *, timezone: str
) -> tuple[pd.Timestamp, pd.Timestamp, str]:
    match = _PRODUCT_NAME.match(product_name.strip())
    if match is None:
        raise ValueError(f"Unsupported regelleistung.net product name `{product_name}`")
    kind, start_hour, end_hour = match.group(1), int(match.group(2)), int(match.group(3))
    start_local = pd.Timestamp(f"{delivery_day} {start_hour:02d}:00").tz_localize(timezone)
    end_day = delivery_day + timedelta(days=1) if end_hour == 24 else delivery_day
    end_local = pd.Timestamp(f"{end_day} {end_hour % 24:02d}:00").tz_localize(timezone)
    direction = {"NEGPOS": "symmetric", "POS": "up", "NEG": "down"}[kind]
    return start_local.tz_convert("UTC"), end_local.tz_convert("UTC"), direction


def parse_regelleistung_fcr_results(
    rows: Sequence[Sequence[str | None]], *, country: str = "GERMANY", timezone: str = GERMANY_TIMEZONE
) -> pd.DataFrame:
    """Parse a regelleistung.net FCR capacity results export into one row per 4-hour product.

    The ``<COUNTRY>_SETTLEMENTCAPACITY_PRICE_[EUR/MW]`` column is the pay-as-cleared price per MW for the whole
    product; it is divided by the product length to obtain EUR/MW/h (3 or 5 hours on DST days).
    """

    frame = _rows_to_frame(rows)
    price_column = f"{country}_SETTLEMENTCAPACITY_PRICE_[EUR/MW]"
    demand_column = f"{country}_DEMAND_[MW]"
    for column in ("DATE_FROM", "PRODUCTNAME", price_column):
        if column not in frame.columns:
            raise ValueError(f"FCR results export is missing column {column}")
    records: list[dict[str, object]] = []
    for row in frame.itertuples(index=False):
        values = dict(zip(frame.columns, row, strict=False))
        delivery_day = _excel_or_iso_date(values["DATE_FROM"])
        start_utc, end_utc, direction = _product_window_utc(delivery_day, str(values["PRODUCTNAME"]), timezone=timezone)
        hours = (end_utc - start_utc).total_seconds() / 3600.0
        price_per_block = _number(values[price_column])
        records.append(
            {
                "delivery_start_utc": start_utc,
                "delivery_end_utc": end_utc,
                "product": "FCR",
                "product_name": str(values["PRODUCTNAME"]),
                "direction": direction,
                "price_eur_per_mw_block": price_per_block,
                "price_eur_per_mw_h": price_per_block / hours,
                "volume_mw": _number(values.get(demand_column)),
                "block_hours": hours,
                "pricing_rule": "pay_as_cleared",
                "source": "regelleistung_net_fcr_results",
            }
        )
    return _deduplicate_products(records)


def parse_regelleistung_afrr_results(
    rows: Sequence[Sequence[str | None]], *, country: str = "GERMANY", timezone: str = GERMANY_TIMEZONE
) -> pd.DataFrame:
    """Parse a regelleistung.net aFRR capacity results export into one row per product and direction.

    aFRR capacity is pay-as-bid: ``price_eur_per_mw_h`` is the published German *average* awarded price (the
    benchmark used here), ``marginal_price_eur_per_mw_h`` is the highest awarded bid.
    """

    frame = _rows_to_frame(rows)
    columns = {
        "average": f"{country}_AVERAGE_CAPACITY_PRICE_[(EUR/MW)/h]",
        "marginal": f"{country}_MARGINAL_CAPACITY_PRICE_[(EUR/MW)/h]",
        "minimum": f"{country}_MIN_CAPACITY_PRICE_[(EUR/MW)/h]",
        "volume": f"{country}_ALLOCATED_VOLUME_[MW]",
    }
    for column in ("DATE_FROM", "PRODUCT", columns["average"], columns["marginal"]):
        if column not in frame.columns:
            raise ValueError(f"aFRR results export is missing column {column}")

    records: list[dict[str, object]] = []
    for row in frame.itertuples(index=False):
        values = dict(zip(frame.columns, row, strict=False))
        delivery_day = _excel_or_iso_date(values["DATE_FROM"])
        start_utc, end_utc, direction = _product_window_utc(delivery_day, str(values["PRODUCT"]), timezone=timezone)
        records.append(
            {
                "delivery_start_utc": start_utc,
                "delivery_end_utc": end_utc,
                "product": "aFRR",
                "product_name": str(values["PRODUCT"]),
                "direction": direction,
                "price_eur_per_mw_h": _number(values[columns["average"]]),
                "marginal_price_eur_per_mw_h": _number(values[columns["marginal"]]),
                "min_price_eur_per_mw_h": _number(values.get(columns["minimum"])),
                "volume_mw": _number(values.get(columns["volume"])),
                "block_hours": (end_utc - start_utc).total_seconds() / 3600.0,
                "pricing_rule": "pay_as_bid_average_benchmark",
                "source": "regelleistung_net_afrr_results",
            }
        )
    return _deduplicate_products(records)


# ---------------------------------------------------------------------------------------------------------------
# Input loading
# ---------------------------------------------------------------------------------------------------------------


def read_market_input(path: str | Path) -> pd.DataFrame:
    """Read a configured German input file (CSV, parquet, Energy-Charts JSON or framework JSON)."""

    target = Path(path)
    suffix = target.suffix.lower()
    if suffix == ".csv":
        return pd.read_csv(target)
    if suffix == ".parquet":
        return pd.read_parquet(target)
    if suffix == ".json":
        with target.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
        if isinstance(payload, dict) and "unix_seconds" in payload:
            return energy_charts_payload_to_frame(payload)
        if isinstance(payload, dict) and "data" in payload:
            return pd.DataFrame(payload["data"])
        return pd.DataFrame(payload)
    raise ValueError(f"Unsupported German input format: {target}")


def _is_block_frame(frame: pd.DataFrame) -> bool:
    return {"delivery_start_utc", "delivery_end_utc"}.issubset(frame.columns)


def _single_source(frame: pd.DataFrame, fallback: str) -> str:
    if "source" not in frame.columns:
        return fallback
    values = sorted(frame["source"].astype(str).unique().tolist())
    return values[0] if len(values) == 1 else fallback


def zero_activation_series(reference: PriceSeries, *, name: str, market: str, unit: str) -> PriceSeries:
    """Zero-valued activation series aligned to ``reference`` (German capacity-only aFRR assumption)."""

    frame = reference.data[["timestamp_utc", "timestamp_local"]].copy()
    frame["market"] = market
    frame["zone"] = reference.zone
    frame["resolution_minutes"] = 15
    frame["price_eur_per_mwh"] = 0.0
    frame["currency"] = unit
    frame["source"] = "germany_capacity_only_assumption"
    frame["value_kind"] = "actual"
    frame["provenance"] = "zero_activation_capacity_only_assumption"
    return PriceSeries(
        name=name,
        market=market,
        zone=reference.zone,
        resolution_minutes=15,
        currency=unit,
        source="germany_capacity_only_assumption",
        value_kind="actual",
        data=frame.reset_index(drop=True),
        metadata={
            "path": None,
            "synthetic": True,
            "reason": "German aFRR is modelled capacity-only: expected activation energy and price are zero",
        },
    )


class GermanyMarketAdapter(MarketAdapter):
    market_id = GERMANY_MARKET_ID
    timezone = GERMANY_TIMEZONE
    resolution_minutes = 15
    supported_workflows = ("da_only", "da_plus_fcr", "da_plus_afrr")
    rejected_workflows: dict[str, str] = {"da_plus_imbalance": GERMANY_IMBALANCE_REJECTION_MESSAGE}
    day_ahead_zone = DE_LU_BIDDING_ZONE
    imbalance_zone = GERMANY_LFC_AREA
    fcr_zone = GERMANY_LFC_AREA
    afrr_zone = GERMANY_LFC_AREA
    reserve_block_minutes = RESERVE_PRODUCT_BLOCK_MINUTES

    def ensure_workflow_supported(self, workflow: str) -> None:
        if workflow in self.rejected_workflows:
            raise ValueError(self.rejected_workflows[workflow])
        if workflow not in self.supported_workflows:
            raise ValueError(f"Unsupported Germany workflow: {workflow}")

    def load_market_series(
        self,
        path: str | Path,
        *,
        name: str,
        market: str,
        zone: str,
        direction: str | None = None,
    ) -> PriceSeries:
        """Load one configured input onto the 15-minute grid (hourly or 4-hour block inputs are expanded)."""

        target = Path(path)
        raw = read_market_input(target)
        source = _single_source(raw, "configured_input")
        if _is_block_frame(raw):
            frame = expand_reserve_blocks(
                raw, market=market, zone=zone, source=source, timezone=self.timezone, direction=direction
            )
        else:
            if direction is not None and "direction" in raw.columns:
                wanted = _normalize_direction(direction)
                raw = raw.loc[raw["direction"].map(_normalize_direction) == wanted].copy()
            frame = normalize_quarter_hour_frame(raw, market=market, zone=zone, source=source, timezone=self.timezone)
        metadata: dict[str, object] = {
            "path": str(target),
            "source_resolutions_minutes": sorted(int(value) for value in frame["source_resolution_minutes"].unique()),
            "expanded_interval_count": int((frame["source_resolution_minutes"] != 15).sum()),
        }
        if market == "day_ahead":
            metadata["sdac_mtu_mismatch_days"] = day_ahead_mtu_mismatch_days(frame, timezone=self.timezone)
            if source == "energy_charts_api":
                metadata["attribution"] = ENERGY_CHARTS_ATTRIBUTION
        return PriceSeries(
            name=name,
            market=market,
            zone=zone,
            resolution_minutes=15,
            source=source,
            value_kind="actual",
            data=frame,
            metadata=metadata,
        )

    def load_actuals(self, config: BacktestConfig) -> LoadedMarketData:
        workflow = config.execution_workflow
        self.ensure_workflow_supported(workflow)
        day_ahead = self.load_market_series(
            config.data.day_ahead.actual_path, name="day_ahead_actual", market="day_ahead", zone=self.day_ahead_zone
        )
        imbalance = None
        if config.data.imbalance is not None:
            imbalance = self.load_market_series(
                config.data.imbalance.actual_path,
                name="rebap_reference_actual",
                market="imbalance",
                zone=self.imbalance_zone,
            )
        fcr_capacity = None
        if config.data.fcr_capacity is not None:
            fcr_capacity = self.load_market_series(
                config.data.fcr_capacity.actual_path,
                name="fcr_capacity_actual",
                market="fcr_capacity",
                zone=self.fcr_zone,
                direction=None,
            )
        capacity: dict[str, PriceSeries | None] = {"up": None, "down": None}
        for direction, series_input in (("up", config.data.afrr_capacity_up), ("down", config.data.afrr_capacity_down)):
            if series_input is not None:
                capacity[direction] = self.load_market_series(
                    series_input.actual_path,
                    name=f"afrr_capacity_{direction}_actual",
                    market=f"afrr_capacity_{direction}",
                    zone=self.afrr_zone,
                    direction=direction,
                )
        activation: dict[str, PriceSeries | None] = {}
        for field_name in (
            "afrr_activation_price_up",
            "afrr_activation_price_down",
            "afrr_activation_ratio_up",
            "afrr_activation_ratio_down",
        ):
            series_input = getattr(config.data, field_name)
            activation[field_name] = (
                self.load_market_series(
                    series_input.actual_path, name=f"{field_name}_actual", market=field_name, zone=self.afrr_zone
                )
                if series_input is not None
                else None
            )
        if workflow == "da_plus_afrr" and self._afrr_capacity_only(config):
            reference = capacity["up"] or capacity["down"]
            if reference is None:
                raise ValueError("German da_plus_afrr requires aFRR capacity inputs")
            for field_name in activation:
                unit = "ratio" if "ratio" in field_name else "EUR"
                activation[field_name] = zero_activation_series(
                    reference, name=f"{field_name}_actual", market=field_name, unit=unit
                )
        return LoadedMarketData(
            day_ahead=day_ahead,
            imbalance=imbalance,
            fcr_capacity=fcr_capacity,
            afrr_capacity_up=capacity["up"],
            afrr_capacity_down=capacity["down"],
            afrr_activation_price_up=activation["afrr_activation_price_up"],
            afrr_activation_price_down=activation["afrr_activation_price_down"],
            afrr_activation_ratio_up=activation["afrr_activation_ratio_up"],
            afrr_activation_ratio_down=activation["afrr_activation_ratio_down"],
        )

    @staticmethod
    def _afrr_capacity_only(config: BacktestConfig) -> bool:
        return config.afrr is None or config.afrr.settlement_mode == "capacity_only"

    def validate_timing(self, config: BacktestConfig) -> None:
        self.ensure_workflow_supported(config.execution_workflow)
        if config.timing.timezone != self.timezone:
            raise ValueError(f"Germany adapter requires timezone={self.timezone}")
        if config.timing.resolution_minutes != self.resolution_minutes:
            raise ValueError("Germany adapter only supports 15-minute resolution")
        gate_closure = pd.Timestamp(f"2000-01-01 {config.timing.day_ahead_gate_closure_local}")
        if gate_closure > pd.Timestamp(f"2000-01-01 {DAY_AHEAD_GATE_CLOSURE_LOCAL}"):
            raise ValueError(
                "Germany adapter requires day_ahead_gate_closure_local <= 12:00 (EPEX/SDAC gate closure, D-1 12:00 CET)"
            )

    def settlement_engine(self, workflow: str) -> SettlementRule:
        self.ensure_workflow_supported(workflow)
        return NoImbalanceSettlement()

    def default_benchmarks(self) -> tuple[str, ...]:
        return ("perfect_foresight", "persistence", "csv")

    def supported_reserve_products(self) -> tuple[str, ...]:
        return ("fcr_symmetric", "afrr_asymmetric")

    def reserve_gate_closure_local(self, workflow: str) -> str | None:
        if workflow == "da_plus_fcr":
            return FCR_GATE_CLOSURE_LOCAL
        if workflow == "da_plus_afrr":
            return AFRR_CAPACITY_GATE_CLOSURE_LOCAL
        return None

    def decision_schedule(self, config: BacktestConfig) -> pd.DataFrame:
        """Decision times per delivery day.

        ``day_ahead_gate_closure_*`` is the column the engine uses as the decision time. For reserve workflows it
        is set to the earlier reserve auction gate closure (FCR D-1 08:00, aFRR D-1 09:00), because the reserve
        commitment is made before the day-ahead auction; the true day-ahead gate closure is kept separately.
        """

        schedule = super().decision_schedule(config)
        reserve_closure = self.reserve_gate_closure_local(config.execution_workflow)
        schedule["day_ahead_market_gate_closure_local"] = schedule["day_ahead_gate_closure_local"]
        schedule["day_ahead_market_gate_closure_utc"] = schedule["day_ahead_gate_closure_utc"]
        schedule["decision_time_basis"] = "day_ahead_gate_closure"
        if reserve_closure is None:
            return schedule
        reserve_local = [
            pd.Timestamp(
                f"{(pd.Timestamp(day) - pd.Timedelta(days=1)).date()} {reserve_closure}",
                tz=self.timezone,
            )
            for day in schedule["delivery_date_local"]
        ]
        decision_local = [
            min(reserve, day_ahead)
            for reserve, day_ahead in zip(reserve_local, schedule["day_ahead_gate_closure_local"], strict=True)
        ]
        schedule["reserve_gate_closure_local"] = reserve_local
        schedule["reserve_gate_closure_utc"] = [value.tz_convert("UTC") for value in reserve_local]
        schedule["day_ahead_gate_closure_local"] = decision_local
        schedule["day_ahead_gate_closure_utc"] = [value.tz_convert("UTC") for value in decision_local]
        schedule["decision_time_basis"] = (
            "fcr_gate_closure" if config.execution_workflow == "da_plus_fcr" else "afrr_capacity_gate_closure"
        )
        return schedule

    def gate_closure_definition(self, config: BacktestConfig) -> str:
        reserve_closure = self.reserve_gate_closure_local(config.execution_workflow)
        text = (
            f"D-1 {config.timing.day_ahead_gate_closure_local} day-ahead gate closure (EPEX/SDAC, {self.timezone}, "
            "15-minute evaluation grid)"
        )
        if reserve_closure is not None:
            product = "FCR" if config.execution_workflow == "da_plus_fcr" else "aFRR capacity"
            text = f"D-1 {reserve_closure} {product} auction gate closure (joint decision time); " + text
        return text

    def build_reserve_product(
        self, config: BacktestConfig
    ) -> SymmetricCapacityReserveProduct | AfrrAsymmetricReserveProduct | None:
        common = {
            "operator": "German TSOs via regelleistung.net",
            "zone": self.fcr_zone,
            "product_block_minutes": RESERVE_PRODUCT_BLOCK_MINUTES,
            "engine_reserve_granularity": "per_quarter_hour_relaxation",
        }
        if config.execution_workflow == "da_plus_fcr" and config.fcr is not None:
            return SymmetricCapacityReserveProduct(
                market_id=self.market_id,
                sustain_duration_minutes=config.fcr.sustain_duration_minutes,
                settlement_assumption=CapacityOnlyReserveSettlement(),
                activation_assumption=NoActivationAssumption(),
                metadata={
                    **common,
                    "cooperation": "FCR Cooperation",
                    "gate_closure_local": f"D-1 {FCR_GATE_CLOSURE_LOCAL}",
                    "pricing_rule": "pay_as_cleared",
                    "ler_energy_hours_per_direction_pq_rule": FCR_LER_ENERGY_HOURS_PER_DIRECTION,
                    "simplified_product_logic": config.fcr.simplified_product_logic,
                },
            )
        if config.execution_workflow == "da_plus_afrr" and config.afrr is not None:
            capacity_only = self._afrr_capacity_only(config)
            return AfrrAsymmetricReserveProduct(
                market_id=self.market_id,
                sustain_duration_minutes=config.afrr.sustain_duration_minutes,
                settlement_assumption=CapacityOnlyReserveSettlement()
                if capacity_only
                else CapacityPlusActivationReserveSettlement(),
                activation_assumption=NoActivationAssumption() if capacity_only else ExpectedActivationAssumption(),
                metadata={
                    **common,
                    "zone": self.afrr_zone,
                    "gate_closure_local": f"D-1 {AFRR_CAPACITY_GATE_CLOSURE_LOCAL}",
                    "pricing_rule": "pay_as_bid",
                    "capacity_price_benchmark": "published German average awarded capacity price",
                    "energy_market": "PICASSO aFRR energy is not modelled" if capacity_only else "user supplied",
                    "ler_energy_hours_per_direction_pq_rule": AFRR_LER_ENERGY_HOURS_PER_DIRECTION,
                    "simplified_product_logic": config.afrr.simplified_product_logic,
                },
            )
        return None

    def settlement_metadata(self, config: BacktestConfig) -> dict[str, object]:
        metadata = super().settlement_metadata(config)
        basis = {"da_plus_fcr": "fcr_gate_closure", "da_plus_afrr": "afrr_capacity_gate_closure"}
        metadata.update(
            {
                "imbalance_price_reference": "reBAP (documented only; deliberate imbalance positions are not permitted)",
                "day_ahead_quarter_hour_mtu_start_local_date": DAY_AHEAD_QUARTER_HOUR_MTU_START.isoformat(),
                "decision_time_basis": basis.get(config.execution_workflow, "day_ahead_gate_closure"),
            }
        )
        if config.execution_workflow in {"da_plus_fcr", "da_plus_afrr"}:
            metadata["reserve_product_block_minutes"] = RESERVE_PRODUCT_BLOCK_MINUTES
            metadata["reserve_block_constraint_in_engine"] = False
        return metadata


# ---------------------------------------------------------------------------------------------------------------
# Block-constant co-optimisation of one delivery day (research path)
# ---------------------------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class GermanBatteryParams:
    power_mw: float = 1.0
    energy_mwh: float = 2.0
    soc_min_mwh: float = 0.1
    soc_max_mwh: float = 1.9
    initial_soc_mwh: float = 1.0
    terminal_soc_mwh: float = 1.0
    charge_efficiency: float = 0.95
    discharge_efficiency: float = 0.95
    degradation_eur_per_mwh_discharged: float = 8.0
    degradation_eur_per_mwh_throughput: float = 0.0

    def __post_init__(self) -> None:
        if not 0.0 <= self.soc_min_mwh < self.soc_max_mwh <= self.energy_mwh:
            raise ValueError("Battery SOC bounds must satisfy 0 <= soc_min < soc_max <= energy")
        for value in (self.initial_soc_mwh, self.terminal_soc_mwh):
            if not self.soc_min_mwh <= value <= self.soc_max_mwh:
                raise ValueError("Initial and terminal SOC must lie inside the SOC bounds")


@dataclass(frozen=True)
class GermanDaySolution:
    charge_mw: np.ndarray
    discharge_mw: np.ndarray
    soc_mwh: np.ndarray
    fcr_mw: np.ndarray
    afrr_up_mw: np.ndarray
    afrr_down_mw: np.ndarray
    objective_eur: float
    solver_name: str


def _as_array(values: Sequence[float] | np.ndarray | None, length: int, label: str) -> np.ndarray | None:
    if values is None:
        return None
    array = np.asarray(values, dtype=float)
    if array.shape != (length,):
        raise ValueError(f"{label} must have shape ({length},), got {array.shape}")
    if not np.isfinite(array).all():
        raise ValueError(f"{label} contains non-finite values")
    return array


def solve_germany_day(
    *,
    day_ahead_price_eur_per_mwh: Sequence[float] | np.ndarray,
    battery: GermanBatteryParams,
    block_index: Sequence[int] | np.ndarray | None = None,
    fcr_price_eur_per_mw_h: Sequence[float] | np.ndarray | None = None,
    afrr_up_price_eur_per_mw_h: Sequence[float] | np.ndarray | None = None,
    afrr_down_price_eur_per_mw_h: Sequence[float] | np.ndarray | None = None,
    fixed_fcr_mw: Sequence[float] | np.ndarray | None = None,
    fixed_afrr_up_mw: Sequence[float] | np.ndarray | None = None,
    fixed_afrr_down_mw: Sequence[float] | np.ndarray | None = None,
    fcr_energy_hours: float = FCR_LER_ENERGY_HOURS_PER_DIRECTION,
    afrr_energy_hours: float = AFRR_LER_ENERGY_HOURS_PER_DIRECTION,
    block_constant_reserve: bool = True,
    soc_reservation_check: Literal["start", "both"] = "both",
    dt_hours: float = 0.25,
) -> GermanDaySolution:
    """Co-optimise one delivery day of DA energy and (optionally) FCR or aFRR capacity (Pyomo + HiGHS MILP).

    Prices are the decision inputs (forecasts); settlement against actuals happens outside. Reserve variables are
    per quarter-hour and, with ``block_constant_reserve=True``, tied to one value per local 4-hour product block.
    Activation energy is assumed to be zero; instead the state of charge must keep ``fcr_energy_hours`` (FCR,
    both directions) and ``afrr_energy_hours`` (aFRR, per direction) of full-power delivery in reserve.
    ``soc_reservation_check="start"`` reproduces the framework solver (check at interval start only);
    ``"both"`` also checks the interval end. A binary mode variable forbids simultaneous charge and discharge.
    """

    from pyomo.environ import (
        Binary,
        ConcreteModel,
        Constraint,
        NonNegativeReals,
        Objective,
        RangeSet,
        Reals,
        SolverFactory,
        Var,
        maximize,
        value,
    )
    from pyomo.opt import SolverStatus, TerminationCondition

    from ...optimization.solver import ensure_solver_available

    prices = np.asarray(day_ahead_price_eur_per_mwh, dtype=float)
    horizon = int(prices.shape[0])
    if horizon == 0 or not np.isfinite(prices).all():
        raise ValueError("Day-ahead prices must be a non-empty finite array")
    fcr_prices = _as_array(fcr_price_eur_per_mw_h, horizon, "fcr_price_eur_per_mw_h")
    up_prices = _as_array(afrr_up_price_eur_per_mw_h, horizon, "afrr_up_price_eur_per_mw_h")
    down_prices = _as_array(afrr_down_price_eur_per_mw_h, horizon, "afrr_down_price_eur_per_mw_h")
    fixed_fcr = _as_array(fixed_fcr_mw, horizon, "fixed_fcr_mw")
    fixed_up = _as_array(fixed_afrr_up_mw, horizon, "fixed_afrr_up_mw")
    fixed_down = _as_array(fixed_afrr_down_mw, horizon, "fixed_afrr_down_mw")
    fcr_enabled = fcr_prices is not None or fixed_fcr is not None
    afrr_enabled = up_prices is not None or down_prices is not None or fixed_up is not None or fixed_down is not None
    blocks = np.zeros(horizon, dtype=int) if block_index is None else np.asarray(block_index, dtype=int)
    if blocks.shape != (horizon,):
        raise ValueError("block_index must align with the day-ahead price array")
    power = float(battery.power_mw)
    eta_c = float(battery.charge_efficiency)
    eta_d = float(battery.discharge_efficiency)

    model = ConcreteModel()
    model.T = RangeSet(0, horizon - 1)
    model.S = RangeSet(0, horizon)
    model.charge = Var(model.T, domain=NonNegativeReals, bounds=(0.0, power))
    model.discharge = Var(model.T, domain=NonNegativeReals, bounds=(0.0, power))
    model.mode = Var(model.T, domain=Binary)
    model.soc = Var(model.S, domain=Reals, bounds=(battery.soc_min_mwh, battery.soc_max_mwh))
    model.fcr = Var(model.T, domain=NonNegativeReals, bounds=(0.0, power))
    model.afrr_up = Var(model.T, domain=NonNegativeReals, bounds=(0.0, power))
    model.afrr_down = Var(model.T, domain=NonNegativeReals, bounds=(0.0, power))
    model.soc[0].fix(battery.initial_soc_mwh)
    model.soc[horizon].fix(battery.terminal_soc_mwh)

    model.charge_mode = Constraint(model.T, rule=lambda m, t: m.charge[t] <= power * m.mode[t])
    model.discharge_mode = Constraint(model.T, rule=lambda m, t: m.discharge[t] <= power * (1 - m.mode[t]))
    model.up_headroom = Constraint(model.T, rule=lambda m, t: m.discharge[t] + m.fcr[t] + m.afrr_up[t] <= power)
    model.down_headroom = Constraint(model.T, rule=lambda m, t: m.charge[t] + m.fcr[t] + m.afrr_down[t] <= power)
    model.soc_balance = Constraint(
        model.T,
        rule=lambda m, t: m.soc[t + 1] == m.soc[t] + (m.charge[t] * eta_c - m.discharge[t] / eta_d) * dt_hours,
    )

    def _reserve_rule(variable: Any, fixed: np.ndarray | None, enabled: bool) -> Any:
        def rule(m: Any, t: int) -> Any:
            if not enabled:
                return variable[t] == 0.0
            if fixed is not None:
                return variable[t] == float(fixed[t])
            return Constraint.Skip

        return rule

    model.fcr_fix = Constraint(model.T, rule=_reserve_rule(model.fcr, fixed_fcr, fcr_enabled))
    model.afrr_up_fix = Constraint(
        model.T,
        rule=_reserve_rule(model.afrr_up, fixed_up, afrr_enabled and (up_prices is not None or fixed_up is not None)),
    )
    model.afrr_down_fix = Constraint(
        model.T,
        rule=_reserve_rule(
            model.afrr_down, fixed_down, afrr_enabled and (down_prices is not None or fixed_down is not None)
        ),
    )
    if block_constant_reserve:
        first_of_block: dict[int, int] = {}
        links: list[tuple[int, int]] = []
        for t in range(horizon):
            block = int(blocks[t])
            if block in first_of_block:
                links.append((first_of_block[block], t))
            else:
                first_of_block[block] = t
        model.L = RangeSet(0, max(len(links) - 1, 0))
        if links:
            model.fcr_block = Constraint(model.L, rule=lambda m, k: m.fcr[links[k][1]] == m.fcr[links[k][0]])
            model.up_block = Constraint(model.L, rule=lambda m, k: m.afrr_up[links[k][1]] == m.afrr_up[links[k][0]])
            model.down_block = Constraint(
                model.L, rule=lambda m, k: m.afrr_down[links[k][1]] == m.afrr_down[links[k][0]]
            )

    check_points = [0] if soc_reservation_check == "start" else [0, 1]

    def _floor(m: Any, t: int, offset: int) -> Any:
        needed = (m.fcr[t] * fcr_energy_hours + m.afrr_up[t] * afrr_energy_hours) / eta_d
        return m.soc[t + offset] - needed >= battery.soc_min_mwh

    def _ceiling(m: Any, t: int, offset: int) -> Any:
        needed = (m.fcr[t] * fcr_energy_hours + m.afrr_down[t] * afrr_energy_hours) * eta_c
        return m.soc[t + offset] + needed <= battery.soc_max_mwh

    model.O = RangeSet(0, len(check_points) - 1)
    if fcr_enabled or afrr_enabled:
        model.reserve_floor = Constraint(model.T, model.O, rule=lambda m, t, o: _floor(m, t, check_points[o]))
        model.reserve_ceiling = Constraint(model.T, model.O, rule=lambda m, t, o: _ceiling(m, t, check_points[o]))

    zero = np.zeros(horizon)
    fcr_objective = fcr_prices if fcr_prices is not None else zero
    up_objective = up_prices if up_prices is not None else zero
    down_objective = down_prices if down_prices is not None else zero
    degradation_discharge = float(battery.degradation_eur_per_mwh_discharged)
    degradation_throughput = float(battery.degradation_eur_per_mwh_throughput)
    model.objective = Objective(
        expr=sum(
            (
                float(prices[t]) * (model.discharge[t] - model.charge[t])
                + float(fcr_objective[t]) * model.fcr[t]
                + float(up_objective[t]) * model.afrr_up[t]
                + float(down_objective[t]) * model.afrr_down[t]
                - degradation_discharge * model.discharge[t]
                - degradation_throughput * (model.charge[t] + model.discharge[t])
            )
            * dt_hours
            for t in range(horizon)
        ),
        sense=maximize,
    )
    solver_name = ensure_solver_available()
    result = SolverFactory(solver_name).solve(model)
    if result.solver.status != SolverStatus.ok or result.solver.termination_condition not in {
        TerminationCondition.optimal,
        TerminationCondition.feasible,
    }:
        raise RuntimeError(
            f"Germany day optimisation failed: status={result.solver.status} "
            f"termination={result.solver.termination_condition}"
        )

    def _values(variable: Any, size: int) -> np.ndarray:
        return np.array([value(variable[index]) for index in range(size)], dtype=float)

    return GermanDaySolution(
        charge_mw=_values(model.charge, horizon),
        discharge_mw=_values(model.discharge, horizon),
        soc_mwh=_values(model.soc, horizon + 1),
        fcr_mw=_values(model.fcr, horizon),
        afrr_up_mw=_values(model.afrr_up, horizon),
        afrr_down_mw=_values(model.afrr_down, horizon),
        objective_eur=float(value(model.objective)),
        solver_name=solver_name,
    )
