"""German reserve capacity auction results from regelleistung.net (FCR and aFRR, capacity market).

Licence finding (checked 2026-10-26, https://www.regelleistung.net/de-de/Impressum, section "Nutzung der
Internet-Seiten"): content is protected by copyright and reproduction requires the prior written consent of the
German TSOs unless permitted by law. The data are therefore **not redistributed**: files are written under
``data/raw/germany/`` (gitignored) and only a manifest stub with ``redistributed: false`` is committed.
"""

from __future__ import annotations

import re
import zipfile
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from xml.etree import ElementTree

import numpy as np
import pandas as pd

from .http import FetchRecord, HttpClient, cached_download, validate_xlsx_file

REGELLEISTUNG_API = "https://www.regelleistung.net/apps/crds/api/v2/tenders"
REGELLEISTUNG_TERMS_URL = "https://www.regelleistung.net/de-de/Impressum"
REGELLEISTUNG_LICENCE = "proprietary (copyright German TSOs; reproduction needs prior written consent)"
REGELLEISTUNG_ATTRIBUTION = "50Hertz, Amprion, TenneT TSO, TransnetBW via regelleistung.net"
LOCAL_TZ = "Europe/Berlin"
_XLSX_NS = "{http://schemas.openxmlformats.org/spreadsheetml/2006/main}"
_PRODUCT_RE = re.compile(r"^(NEGPOS|POS|NEG)_(\d{2})_(\d{2})$")


@dataclass(frozen=True)
class DayFile:
    product: str
    kind: str
    day: pd.Timestamp
    path: Path
    record: FetchRecord


def read_xlsx_rows(path: Path) -> list[list[str | None]]:
    """Read the first worksheet of an xlsx file with the standard library (no openpyxl dependency)."""

    with zipfile.ZipFile(path) as archive:
        names = set(archive.namelist())
        shared: list[str] = []
        if "xl/sharedStrings.xml" in names:
            root = ElementTree.fromstring(archive.read("xl/sharedStrings.xml"))
            for item in root.iter(f"{_XLSX_NS}si"):
                shared.append("".join(node.text or "" for node in item.iter(f"{_XLSX_NS}t")))
        sheets = sorted(n for n in names if n.startswith("xl/worksheets/sheet") and n.endswith(".xml"))
        if not sheets:
            return []
        root = ElementTree.fromstring(archive.read(sheets[0]))
    rows: list[list[str | None]] = []
    for row in root.iter(f"{_XLSX_NS}row"):
        values: dict[int, str | None] = {}
        for cell in row.findall(f"{_XLSX_NS}c"):
            ref = cell.get("r") or ""
            letters = re.match(r"[A-Z]+", ref)
            if letters is None:
                continue
            index = 0
            for char in letters.group(0):
                index = index * 26 + (ord(char) - 64)
            kind = cell.get("t")
            value_node = cell.find(f"{_XLSX_NS}v")
            value: str | None
            if kind == "s" and value_node is not None and value_node.text is not None:
                value = shared[int(value_node.text)]
            elif kind == "inlineStr":
                value = "".join(node.text or "" for node in cell.iter(f"{_XLSX_NS}t"))
            else:
                value = value_node.text if value_node is not None else None
            values[index - 1] = value
        if values:
            width = max(values) + 1
            rows.append([values.get(i) for i in range(width)])
    return rows


def xlsx_frame(path: Path) -> pd.DataFrame:
    rows = read_xlsx_rows(path)
    if not rows:
        return pd.DataFrame()
    header = [str(h) if h is not None else f"col_{i}" for i, h in enumerate(rows[0])]
    body = [r + [None] * (len(header) - len(r)) for r in rows[1:]]
    return pd.DataFrame([r[: len(header)] for r in body], columns=header)


def excel_serial_to_date(value: object) -> pd.Timestamp:
    return pd.Timestamp("1899-12-30") + pd.to_timedelta(float(str(value)), unit="D")


def to_number(series: pd.Series) -> pd.Series:
    return pd.to_numeric(series.replace({"-": None, "": None}), errors="coerce")


def fetch_day(
    client: HttpClient,
    raw_dir: Path,
    product: str,
    kind: str,
    day: pd.Timestamp,
    *,
    refresh: bool = False,
    offline: bool = False,
) -> DayFile:
    """Fetch the ``results`` (aggregated) or ``demands`` xlsx of one delivery day for FCR or aFRR."""

    endpoint = {"results": "results/aggregated", "demands": "demands"}[kind]
    params = {
        "deliveryDate": f"{day:%Y-%m-%d}",
        "productType": product,
        "market": "CAPACITY",
        "exportFormat": "xlsx",
    }
    dest = raw_dir / "germany" / "regelleistung" / product / kind / f"{product}_{kind}_{day:%Y-%m-%d}.xlsx"
    period_end = (day + pd.Timedelta(days=1)).tz_localize(LOCAL_TZ).tz_convert("UTC")
    record = cached_download(
        client,
        f"{REGELLEISTUNG_API}/{endpoint}",
        dest,
        params=params,
        validate=validate_xlsx_file,
        refresh=refresh,
        period_end_utc=period_end,
        settle=pd.Timedelta(days=1),
        offline=offline,
    )
    return DayFile(product, kind, day, dest, record)


def _block_bounds(day: pd.Timestamp, start_hour: int, end_hour: int) -> tuple[pd.Timestamp, pd.Timestamp]:
    start_local = (day + pd.Timedelta(hours=start_hour)).tz_localize(LOCAL_TZ)
    end_local = (day + pd.Timedelta(hours=end_hour)).tz_localize(LOCAL_TZ)
    return start_local.tz_convert("UTC"), end_local.tz_convert("UTC")


def _parse_product(name: str) -> tuple[str, int, int] | None:
    match = _PRODUCT_RE.match(name.strip())
    if match is None:
        return None
    direction = {"NEGPOS": "symmetric", "POS": "up", "NEG": "down"}[match.group(1)]
    return direction, int(match.group(2)), int(match.group(3))


GERMANY_COLUMNS = [
    "delivery_start_utc",
    "delivery_end_utc",
    "product",
    "direction",
    "price_eur_per_mw_h",
    "volume_mw",
    "marginal_price_eur_per_mw_h",
]


def parse_fcr_results(frame: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Return (Germany rows in the contract layout, all-country long table) from one FCR results file."""

    if frame.empty:
        return pd.DataFrame(columns=GERMANY_COLUMNS), pd.DataFrame()
    germany_rows: list[dict[str, object]] = []
    country_rows: list[dict[str, object]] = []
    countries = sorted(
        {c[: -len("_DEMAND_[MW]")] for c in frame.columns if c.endswith("_DEMAND_[MW]")},
    )
    for _, row in frame.iterrows():
        parsed = _parse_product(str(row.get("PRODUCTNAME", "")))
        if parsed is None:
            continue
        direction, h0, h1 = parsed
        day = excel_serial_to_date(row["DATE_FROM"])
        start, end = _block_bounds(day, h0, h1)
        hours = (end - start).total_seconds() / 3600.0
        tender = to_number(pd.Series([row.get("TENDER_NUMBER")])).iloc[0]
        for country in countries:
            demand = to_number(pd.Series([row.get(f"{country}_DEMAND_[MW]")])).iloc[0]
            price = to_number(pd.Series([row.get(f"{country}_SETTLEMENTCAPACITY_PRICE_[EUR/MW]")])).iloc[0]
            balance = to_number(pd.Series([row.get(f"{country}_DEFICIT(-)_SURPLUS(+)_[MW]")])).iloc[0]
            if pd.isna(demand) and pd.isna(price):
                continue
            country_rows.append(
                {
                    "delivery_start_utc": start,
                    "delivery_end_utc": end,
                    "tender_number": tender,
                    "country": country.lower(),
                    "demand_mw": demand,
                    "price_eur_per_mw": price,
                    "price_eur_per_mw_h": price / hours if pd.notna(price) else np.nan,
                    "deficit_surplus_mw": balance,
                }
            )
            if country == "GERMANY" and pd.notna(price):
                germany_rows.append(
                    {
                        "delivery_start_utc": start,
                        "delivery_end_utc": end,
                        "product": "FCR",
                        "direction": direction,
                        "price_eur_per_mw_h": price / hours,
                        "volume_mw": demand,
                        "marginal_price_eur_per_mw_h": price / hours,
                    }
                )
    return pd.DataFrame(germany_rows, columns=GERMANY_COLUMNS), pd.DataFrame(country_rows)


def parse_afrr_results(results: pd.DataFrame, demands: pd.DataFrame | None) -> pd.DataFrame:
    """Germany aFRR capacity rows: average and marginal capacity price, procured German block demand."""

    if results.empty:
        return pd.DataFrame(columns=GERMANY_COLUMNS)
    demand_by_product: dict[str, float] = {}
    if demands is not None and not demands.empty and "PRODUCT" in demands.columns:
        column = "GERMANY_BLOCK_DEMAND_[MW]" if "GERMANY_BLOCK_DEMAND_[MW]" in demands.columns else None
        if column is not None:
            values = to_number(demands[column])
            for product, value in zip(demands["PRODUCT"].astype(str), values, strict=True):
                if pd.notna(value):
                    demand_by_product[product.strip()] = float(value)
    rows: list[dict[str, object]] = []
    for _, row in results.iterrows():
        name = str(row.get("PRODUCT", ""))
        parsed = _parse_product(name)
        if parsed is None:
            continue
        direction, h0, h1 = parsed
        day = excel_serial_to_date(row["DATE_FROM"])
        start, end = _block_bounds(day, h0, h1)
        average = to_number(pd.Series([row.get("GERMANY_AVERAGE_CAPACITY_PRICE_[(EUR/MW)/h]")])).iloc[0]
        marginal = to_number(pd.Series([row.get("GERMANY_MARGINAL_CAPACITY_PRICE_[(EUR/MW)/h]")])).iloc[0]
        volume = demand_by_product.get(name.strip(), np.nan)
        if pd.isna(volume) and "GERMANY_ALLOCATED_VOLUME_[MW]" in results.columns:
            volume = to_number(pd.Series([row.get("GERMANY_ALLOCATED_VOLUME_[MW]")])).iloc[0]
        if pd.isna(average) and pd.isna(marginal):
            continue
        rows.append(
            {
                "delivery_start_utc": start,
                "delivery_end_utc": end,
                "product": "aFRR",
                "direction": direction,
                "price_eur_per_mw_h": average,
                "volume_mw": volume,
                "marginal_price_eur_per_mw_h": marginal,
            }
        )
    return pd.DataFrame(rows, columns=GERMANY_COLUMNS)


def finalize_germany(frames: Sequence[pd.DataFrame]) -> pd.DataFrame:
    usable = [f for f in frames if not f.empty]
    if not usable:
        frame = pd.DataFrame(columns=GERMANY_COLUMNS)
    else:
        frame = pd.concat(usable, ignore_index=True)
    frame["delivery_start_utc"] = pd.to_datetime(frame["delivery_start_utc"], utc=True)
    frame["delivery_end_utc"] = pd.to_datetime(frame["delivery_end_utc"], utc=True)
    for column in ("price_eur_per_mw_h", "volume_mw", "marginal_price_eur_per_mw_h"):
        frame[column] = pd.to_numeric(frame[column], errors="coerce").astype("float32")
    frame = frame.drop_duplicates(["delivery_start_utc", "product", "direction"], keep="last")
    return frame.sort_values(["delivery_start_utc", "direction"]).reset_index(drop=True)[GERMANY_COLUMNS]
