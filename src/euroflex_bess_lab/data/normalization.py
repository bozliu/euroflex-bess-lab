from __future__ import annotations

import json
import xml.etree.ElementTree as ET
from collections.abc import Iterable
from typing import Literal, cast

import pandas as pd

from ..time_utils import expand_to_resolution, resolution_code_to_minutes
from ..types import PriceSeries

ENTSOE_NS = {"ns": "urn:iec62325.351:tc57wg16:451-3:publicationdocument:7:3"}


def _coerce_optional_float(value: object, fallback: float) -> float:
    if value in {None, ""}:
        return fallback
    if isinstance(value, (int, float, str)):
        return float(value)
    raise ValueError(f"Expected a numeric TenneT price field, got {type(value).__name__}")


def _coerce_nullable_float(value: object) -> float | None:
    if value in {None, ""}:
        return None
    if isinstance(value, (int, float, str)):
        return float(value)
    raise ValueError(f"Expected a numeric TenneT price field, got {type(value).__name__}")


def _extract_tennet_series_rows(payload: dict[str, object]) -> list[dict[str, object]]:
    series_rows = payload.get("TimeSeries", [])
    if not series_rows and isinstance(payload.get("Response"), dict):
        response_wrapper = cast(dict[str, object], payload["Response"])
        series_rows = response_wrapper.get("TimeSeries", [])
    if not isinstance(series_rows, Iterable):
        raise ValueError("TenneT payload does not contain a valid TimeSeries collection")
    return list(series_rows)


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
    source_timezone: str = "UTC",
    zone: str = "10YNL----------L",
) -> PriceSeries:
    payload = json.loads(json_payload) if isinstance(json_payload, str) else json_payload
    series_rows = _extract_tennet_series_rows(payload)

    rows: list[dict[str, object]] = []
    for time_series in series_rows:
        period = cast(dict[str, object], time_series.get("Period", {}))
        points = period.get("Points", [])
        if not isinstance(points, Iterable):
            continue
        for point in points:
            start = pd.Timestamp(point["timeInterval_start"], tz=source_timezone).tz_convert("UTC")
            end = pd.Timestamp(point["timeInterval_end"], tz=source_timezone).tz_convert("UTC")
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
                    "dispatch_up_price_eur_per_mwh": _coerce_optional_float(point.get("dispatch_up"), shortage),
                    "dispatch_down_price_eur_per_mwh": _coerce_optional_float(point.get("dispatch_down"), surplus),
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


def normalize_tennet_merit_order_list_json(
    json_payload: str | dict[str, object],
    *,
    value_kind: Literal["actual", "forecast"] = "actual",
    source: str = "tennet_merit_order_list",
    local_timezone: str = "Europe/Amsterdam",
    source_timezone: str = "UTC",
    zone: str = "10YNL----------L",
) -> pd.DataFrame:
    payload = json.loads(json_payload) if isinstance(json_payload, str) else json_payload
    series_rows = _extract_tennet_series_rows(payload)

    rows: list[dict[str, object]] = []
    for time_series in series_rows:
        period = cast(dict[str, object], time_series.get("Period", {}))
        points = period.get("Points", [])
        if not isinstance(points, Iterable):
            continue
        for point in points:
            start = pd.Timestamp(point["timeInterval_start"], tz=source_timezone).tz_convert("UTC")
            end = pd.Timestamp(point["timeInterval_end"], tz=source_timezone).tz_convert("UTC")
            thresholds = point.get("Thresholds", [])
            if not isinstance(thresholds, Iterable):
                continue
            for threshold in thresholds:
                if not isinstance(threshold, dict):
                    continue
                rows.append(
                    {
                        "timestamp_utc": start,
                        "timestamp_local": start.tz_convert(local_timezone),
                        "market": "afrr_merit_order",
                        "zone": zone,
                        "resolution_minutes": int((end - start).total_seconds() / 60),
                        "isp": int(point.get("isp", 0)),
                        "threshold_mw": float(threshold["capacity_threshold"]),
                        "price_up_eur_per_mwh": _coerce_nullable_float(threshold.get("price_up")),
                        "price_down_eur_per_mwh": _coerce_nullable_float(threshold.get("price_down")),
                        "currency": "EUR",
                        "source": source,
                        "value_kind": value_kind,
                        "provenance": "tennet_publication_v1_merit_order",
                    }
                )

    frame = pd.DataFrame(rows).sort_values(["timestamp_utc", "threshold_mw"]).reset_index(drop=True)
    if frame.empty:
        raise ValueError("TenneT payload did not contain any merit-order threshold rows")
    if int(frame["resolution_minutes"].iloc[0]) != 15:
        raise ValueError("TenneT merit-order normalization only supports 15-minute intervals in the MVP")
    return frame


def normalize_tennet_frequency_restoration_reserve_activations_json(
    json_payload: str | dict[str, object],
    *,
    value_kind: Literal["actual", "forecast"] = "actual",
    source: str = "tennet_frequency_restoration_reserve_activations",
    local_timezone: str = "Europe/Amsterdam",
    source_timezone: str = "UTC",
    zone: str = "10YNL----------L",
) -> pd.DataFrame:
    payload = json.loads(json_payload) if isinstance(json_payload, str) else json_payload
    series_rows = _extract_tennet_series_rows(payload)

    rows: list[dict[str, object]] = []
    for time_series in series_rows:
        period = cast(dict[str, object], time_series.get("Period", {}))
        points = period.get("Points", [])
        if not isinstance(points, Iterable):
            continue
        for point in points:
            start = pd.Timestamp(point["timeInterval_start"], tz=source_timezone).tz_convert("UTC")
            end = pd.Timestamp(point["timeInterval_end"], tz=source_timezone).tz_convert("UTC")
            rows.append(
                {
                    "timestamp_utc": start,
                    "timestamp_local": start.tz_convert(local_timezone),
                    "market": "afrr_activation_volume",
                    "zone": zone,
                    "resolution_minutes": int((end - start).total_seconds() / 60),
                    "isp": int(point.get("isp", 0)),
                    "afrr_up_mw": float(point.get("aFRR_up", 0.0)),
                    "afrr_down_mw": float(point.get("aFRR_down", 0.0)),
                    "mfrrda_volume_up_mw": float(point.get("mfrrda_volume_up", 0.0)),
                    "mfrrda_volume_down_mw": float(point.get("mfrrda_volume_down", 0.0)),
                    "total_volume_mw": float(point.get("total_volume", 0.0)),
                    "absolute_total_volume_mw": float(point.get("absolute_total_volume", 0.0)),
                    "currency": "MW",
                    "source": source,
                    "value_kind": value_kind,
                    "provenance": "tennet_publication_v1_frequency_restoration_reserve_activations",
                }
            )

    frame = pd.DataFrame(rows).sort_values("timestamp_utc").reset_index(drop=True)
    if frame.empty:
        raise ValueError("TenneT payload did not contain any frequency restoration reserve activation rows")
    if int(frame["resolution_minutes"].iloc[0]) != 15:
        raise ValueError("TenneT activation normalization only supports 15-minute intervals in the MVP")
    return frame


def _select_threshold_price(thresholds: pd.DataFrame, *, volume_mw: float, price_column: str) -> float:
    if volume_mw <= 0:
        return 0.0
    ladder = thresholds[["threshold_mw", price_column]].dropna().sort_values("threshold_mw").reset_index(drop=True)
    if ladder.empty:
        return 0.0
    matched = ladder[ladder["threshold_mw"] >= volume_mw]
    if matched.empty:
        return float(ladder.iloc[-1][price_column])
    return float(matched.iloc[0][price_column])


def _max_available_threshold(thresholds: pd.DataFrame, *, price_column: str) -> float:
    ladder = thresholds[["threshold_mw", price_column]].dropna().sort_values("threshold_mw").reset_index(drop=True)
    if ladder.empty:
        return 0.0
    return float(ladder["threshold_mw"].max())


def _build_tennet_derived_series(
    *,
    name: str,
    market: str,
    zone: str,
    source: str,
    frame: pd.DataFrame,
    value_column: str,
    currency: str,
    metadata: dict[str, object],
) -> PriceSeries:
    series_frame = frame[
        [
            "timestamp_utc",
            "timestamp_local",
            "resolution_minutes",
            value_column,
            "provenance",
            "activation_volume_mw",
            "available_threshold_mw",
        ]
    ].copy()
    series_frame.rename(columns={value_column: "price_eur_per_mwh"}, inplace=True)
    series_frame["market"] = market
    series_frame["zone"] = zone
    series_frame["currency"] = currency
    series_frame["source"] = source
    series_frame["value_kind"] = "actual"
    series_frame = series_frame[
        [
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
            "activation_volume_mw",
            "available_threshold_mw",
        ]
    ]
    return PriceSeries(
        name=name,
        market=market,
        zone=zone,
        resolution_minutes=15,
        currency=currency,
        source=source,
        value_kind="actual",
        data=series_frame,
        metadata=metadata,
    )


def derive_tennet_afrr_activation_series(
    merit_order_frame: pd.DataFrame,
    activations_frame: pd.DataFrame,
    *,
    zone: str = "10YNL----------L",
    source: str = "tennet_live_derived",
    local_timezone: str = "Europe/Amsterdam",
) -> dict[str, PriceSeries]:
    merit_order = merit_order_frame.copy()
    activations = activations_frame.copy()
    merit_order["timestamp_utc"] = pd.to_datetime(merit_order["timestamp_utc"], utc=True, format="mixed")
    merit_order["timestamp_local"] = pd.to_datetime(
        merit_order["timestamp_local"], utc=True, format="mixed"
    ).dt.tz_convert(local_timezone)
    activations["timestamp_utc"] = pd.to_datetime(activations["timestamp_utc"], utc=True, format="mixed")
    activations["timestamp_local"] = pd.to_datetime(
        activations["timestamp_local"], utc=True, format="mixed"
    ).dt.tz_convert(local_timezone)

    rows: list[dict[str, object]] = []
    grouped_thresholds = merit_order.groupby("timestamp_utc", sort=True)
    for row in activations.sort_values("timestamp_utc").itertuples(index=False):
        timestamp_utc = pd.Timestamp(row.timestamp_utc)
        if timestamp_utc not in grouped_thresholds.groups:
            raise ValueError("Cannot derive TenneT aFRR activation series without matching merit-order rows")
        thresholds = grouped_thresholds.get_group(timestamp_utc)
        up_volume = max(float(row.afrr_up_mw), 0.0)
        down_volume = max(abs(float(row.afrr_down_mw)), 0.0)
        up_capacity = _max_available_threshold(thresholds, price_column="price_up_eur_per_mwh")
        down_capacity = _max_available_threshold(thresholds, price_column="price_down_eur_per_mwh")
        rows.append(
            {
                "timestamp_utc": timestamp_utc,
                "timestamp_local": pd.Timestamp(row.timestamp_local).tz_convert(local_timezone),
                "resolution_minutes": int(row.resolution_minutes),
                "activation_price_up_eur_per_mwh": _select_threshold_price(
                    thresholds, volume_mw=up_volume, price_column="price_up_eur_per_mwh"
                ),
                "activation_price_down_eur_per_mwh": _select_threshold_price(
                    thresholds, volume_mw=down_volume, price_column="price_down_eur_per_mwh"
                ),
                "activation_ratio_up": 0.0 if up_capacity <= 0 else min(up_volume / up_capacity, 1.0),
                "activation_ratio_down": 0.0 if down_capacity <= 0 else min(down_volume / down_capacity, 1.0),
                "activation_volume_up_mw": up_volume,
                "activation_volume_down_mw": down_volume,
                "available_up_threshold_mw": up_capacity,
                "available_down_threshold_mw": down_capacity,
                "provenance": "derived_from_tennet_merit_order_and_activations",
            }
        )

    if not rows:
        raise ValueError("Cannot derive TenneT aFRR activation series from empty inputs")
    derived_frame = pd.DataFrame(rows).sort_values("timestamp_utc").reset_index(drop=True)
    return {
        "afrr_activation_price_up": _build_tennet_derived_series(
            name="tennet_afrr_activation_price_up",
            market="afrr_activation_price_up",
            zone=zone,
            source=source,
            frame=derived_frame.rename(
                columns={
                    "activation_volume_up_mw": "activation_volume_mw",
                    "available_up_threshold_mw": "available_threshold_mw",
                }
            ),
            value_column="activation_price_up_eur_per_mwh",
            currency="EUR",
            metadata={
                "publication": "derived_from_merit_order_and_frequency_restoration_reserve_activations",
                "value_unit": "eur_per_mwh",
            },
        ),
        "afrr_activation_price_down": _build_tennet_derived_series(
            name="tennet_afrr_activation_price_down",
            market="afrr_activation_price_down",
            zone=zone,
            source=source,
            frame=derived_frame.rename(
                columns={
                    "activation_volume_down_mw": "activation_volume_mw",
                    "available_down_threshold_mw": "available_threshold_mw",
                }
            ),
            value_column="activation_price_down_eur_per_mwh",
            currency="EUR",
            metadata={
                "publication": "derived_from_merit_order_and_frequency_restoration_reserve_activations",
                "value_unit": "eur_per_mwh",
            },
        ),
        "afrr_activation_ratio_up": _build_tennet_derived_series(
            name="tennet_afrr_activation_ratio_up",
            market="afrr_activation_ratio_up",
            zone=zone,
            source=source,
            frame=derived_frame.rename(
                columns={
                    "activation_volume_up_mw": "activation_volume_mw",
                    "available_up_threshold_mw": "available_threshold_mw",
                }
            ),
            value_column="activation_ratio_up",
            currency="ratio",
            metadata={
                "publication": "derived_from_merit_order_and_frequency_restoration_reserve_activations",
                "value_unit": "unitless_ratio",
            },
        ),
        "afrr_activation_ratio_down": _build_tennet_derived_series(
            name="tennet_afrr_activation_ratio_down",
            market="afrr_activation_ratio_down",
            zone=zone,
            source=source,
            frame=derived_frame.rename(
                columns={
                    "activation_volume_down_mw": "activation_volume_mw",
                    "available_down_threshold_mw": "available_threshold_mw",
                }
            ),
            value_column="activation_ratio_down",
            currency="ratio",
            metadata={
                "publication": "derived_from_merit_order_and_frequency_restoration_reserve_activations",
                "value_unit": "unitless_ratio",
            },
        ),
    }
