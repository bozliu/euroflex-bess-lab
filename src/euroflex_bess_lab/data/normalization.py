from __future__ import annotations

import json
import xml.etree.ElementTree as ET
from collections.abc import Iterable
from typing import Literal

import pandas as pd

from ..time_utils import expand_to_resolution, resolution_code_to_minutes
from ..types import PriceSeries

ENTSOE_NS = {"ns": "urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:3"}


def normalize_entsoe_day_ahead_xml(
    xml_payload: str,
    *,
    zone: str = "10YBE----------2",
    value_kind: Literal["actual", "forecast"] = "actual",
    target_resolution_minutes: int = 15,
    source: str = "entsoe_transparency",
    local_timezone: str = "Europe/Brussels",
) -> PriceSeries:
    root = ET.fromstring(xml_payload)
    rows: list[dict[str, object]] = []
    source_resolution_minutes: int | None = None
    for period in root.findall(".//ns:TimeSeries/ns:Period", ENTSOE_NS):
        start = pd.Timestamp(period.findtext("ns:timeInterval/ns:start", namespaces=ENTSOE_NS), tz="UTC")
        resolution_code = period.findtext("ns:resolution", namespaces=ENTSOE_NS)
        if resolution_code is None:
            raise ValueError("ENTSO-E payload is missing period resolution")
        source_resolution_minutes = resolution_code_to_minutes(resolution_code)
        for point in period.findall("ns:Point", ENTSOE_NS):
            position_text = point.findtext("ns:position", namespaces=ENTSOE_NS)
            price_text = point.findtext("ns:price.amount", namespaces=ENTSOE_NS)
            if position_text is None or price_text is None:
                raise ValueError("ENTSO-E payload is missing point position or price")
            position = int(position_text)
            price = float(price_text)
            timestamp_utc = start + pd.Timedelta(minutes=(position - 1) * source_resolution_minutes)
            rows.append(
                {
                    "timestamp_utc": timestamp_utc,
                    "timestamp_local": timestamp_utc.tz_convert(local_timezone),
                    "market": "day_ahead",
                    "zone": zone,
                    "resolution_minutes": source_resolution_minutes,
                    "price_eur_per_mwh": price,
                    "currency": "EUR",
                    "source": source,
                    "value_kind": value_kind,
                    "provenance": f"source_{resolution_code.lower()}",
                }
            )

    frame = pd.DataFrame(rows).sort_values("timestamp_utc").reset_index(drop=True)
    if frame.empty:
        raise ValueError("ENTSO-E payload did not contain any day-ahead points")
    if source_resolution_minutes is None:
        raise ValueError("ENTSO-E payload resolution could not be determined")
    if source_resolution_minutes != target_resolution_minutes:
        frame = expand_to_resolution(
            frame, source_resolution_minutes, target_resolution_minutes, timezone=local_timezone
        )
    return PriceSeries(
        name="entsoe_day_ahead_prices",
        market="day_ahead",
        zone=zone,
        resolution_minutes=target_resolution_minutes,
        source=source,
        value_kind=value_kind,
        data=frame,
        metadata={"source_resolution_minutes": source_resolution_minutes},
    )


def normalize_elia_imbalance_json(
    json_payload: str | dict[str, object],
    *,
    value_kind: Literal["actual", "forecast"] = "actual",
    target_resolution_minutes: int = 15,
    source: str = "elia_open_data",
    local_timezone: str = "Europe/Brussels",
) -> PriceSeries:
    payload = json.loads(json_payload) if isinstance(json_payload, str) else json_payload
    rows = payload.get("results", [])
    if not isinstance(rows, Iterable):
        raise ValueError("Elia payload does not contain a valid results collection")
    frame = pd.DataFrame(rows)
    if frame.empty:
        raise ValueError("Elia payload does not contain any imbalance price rows")
    frame["timestamp_utc"] = pd.to_datetime(frame["datetime"], utc=True)
    frame["timestamp_local"] = frame["timestamp_utc"].dt.tz_convert(local_timezone)
    frame["market"] = "imbalance"
    frame["zone"] = "10YBE----------2"
    frame["resolution_minutes"] = frame["resolutioncode"].map(resolution_code_to_minutes)
    frame["price_eur_per_mwh"] = frame["imbalanceprice"].astype(float)
    frame["currency"] = "EUR"
    frame["source"] = source
    frame["value_kind"] = value_kind
    frame["provenance"] = frame["resolutioncode"].str.lower().map(lambda code: f"source_{code}")
    frame["quality_status"] = frame["qualitystatus"]
    frame["system_imbalance_mw"] = frame["systemimbalance"]
    frame["marginal_incremental_price_eur_per_mwh"] = frame["marginalincrementalprice"]
    frame["marginal_decremental_price_eur_per_mwh"] = frame["marginaldecrementalprice"]
    frame["alpha_eur_per_mwh"] = frame["alpha"]
    frame["alpha_prime_eur_per_mwh"] = frame["alpha_prime"]
    frame = frame.sort_values("timestamp_utc").reset_index(drop=True)
    if int(frame["resolution_minutes"].iloc[0]) != target_resolution_minutes:
        raise ValueError("Elia imbalance normalization only supports native 15-minute data in the MVP")
    keep = [
        "timestamp_utc",
        "timestamp_local",
        "market",
        "zone",
        "resolution_minutes",
        "price_eur_per_mwh",
        "currency",
        "source",
        "value_kind",
        "provenance",
        "quality_status",
        "system_imbalance_mw",
        "marginal_incremental_price_eur_per_mwh",
        "marginal_decremental_price_eur_per_mwh",
        "alpha_eur_per_mwh",
        "alpha_prime_eur_per_mwh",
    ]
    frame = frame[keep]
    return PriceSeries(
        name="elia_imbalance_prices",
        market="imbalance",
        zone="10YBE----------2",
        resolution_minutes=target_resolution_minutes,
        source=source,
        value_kind=value_kind,
        data=frame,
        metadata={"dataset_id": payload.get("dataset_id", "ods162")},
    )


def normalize_tennet_settlement_prices_json(
    json_payload: str | dict[str, object],
    *,
    value_kind: Literal["actual", "forecast"] = "actual",
    source: str = "tennet_settlement_prices",
    local_timezone: str = "Europe/Amsterdam",
    zone: str = "10YNL----------L",
) -> PriceSeries:
    payload = json.loads(json_payload) if isinstance(json_payload, str) else json_payload
    series_rows = payload.get("TimeSeries", [])
    if not isinstance(series_rows, Iterable):
        raise ValueError("TenneT payload does not contain a valid TimeSeries collection")

    rows: list[dict[str, object]] = []
    for time_series in series_rows:
        period = time_series.get("Period", {})
        points = period.get("Points", [])
        if not isinstance(points, Iterable):
            continue
        for point in points:
            start = pd.Timestamp(point["timeInterval_start"], tz=local_timezone).tz_convert("UTC")
            end = pd.Timestamp(point["timeInterval_end"], tz=local_timezone).tz_convert("UTC")
            shortage = float(point["shortage"])
            surplus = float(point["surplus"])
            rows.append(
                {
                    "timestamp_utc": start,
                    "timestamp_local": start.tz_convert(local_timezone),
                    "market": "imbalance",
                    "zone": zone,
                    "resolution_minutes": int((end - start).total_seconds() / 60),
                    "price_eur_per_mwh": (shortage + surplus) / 2.0,
                    "imbalance_shortage_price_eur_per_mwh": shortage,
                    "imbalance_surplus_price_eur_per_mwh": surplus,
                    "dispatch_up_price_eur_per_mwh": float(point.get("dispatch_up", shortage)),
                    "dispatch_down_price_eur_per_mwh": float(point.get("dispatch_down", surplus)),
                    "regulation_state": point.get("regulation_state"),
                    "regulating_condition": point.get("regulating_condition"),
                    "currency": "EUR",
                    "source": source,
                    "value_kind": value_kind,
                    "provenance": "tennet_publication_v1",
                }
            )

    frame = pd.DataFrame(rows).sort_values("timestamp_utc").reset_index(drop=True)
    if frame.empty:
        raise ValueError("TenneT payload did not contain any settlement price rows")
    if int(frame["resolution_minutes"].iloc[0]) != 15:
        raise ValueError("TenneT settlement normalization only supports 15-minute intervals in the MVP")
    return PriceSeries(
        name="tennet_settlement_prices",
        market="imbalance",
        zone=zone,
        resolution_minutes=15,
        source=source,
        value_kind=value_kind,
        data=frame,
        metadata={"publication": "settlement_prices_v1"},
    )
