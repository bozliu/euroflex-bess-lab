from __future__ import annotations

import json
import math
from pathlib import Path
from typing import Any, Literal

import pandas as pd

from ..types import PriceSeries
from .normalization import (
    normalize_elia_imbalance_json,
    normalize_entsoe_day_ahead_xml,
    normalize_tennet_settlement_prices_json,
)


def _read_frame(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".parquet":
        return pd.read_parquet(path)
    if suffix == ".csv":
        return pd.read_csv(path)
    if suffix == ".json":
        with path.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
        if isinstance(payload, dict) and "data" in payload:
            return pd.DataFrame(payload["data"])
        return pd.DataFrame(payload)
    raise ValueError(f"Unsupported input format: {path}")


def load_price_series(
    path: str | Path,
    *,
    name: str,
    market: str,
    zone: str,
    source: str,
    timezone: str = "Europe/Brussels",
    value_kind: Literal["actual", "forecast"] = "actual",
) -> PriceSeries:
    target = Path(path)
    suffix = target.suffix.lower()

    if suffix == ".xml":
        return normalize_entsoe_day_ahead_xml(
            target.read_text(encoding="utf-8"),
            zone=zone,
            value_kind=value_kind,
            source=source,
            local_timezone=timezone,
        )

    if suffix == ".json":
        with target.open("r", encoding="utf-8") as handle:
            payload = json.load(handle)
        if isinstance(payload, dict) and "results" in payload:
            return normalize_elia_imbalance_json(
                payload,
                value_kind=value_kind,
                source=source,
                local_timezone=timezone,
            )
        if isinstance(payload, dict) and "TimeSeries" in payload:
            return normalize_tennet_settlement_prices_json(
                payload,
                value_kind=value_kind,
                source=source,
                local_timezone=timezone,
                zone=zone,
            )

    frame = _read_frame(target)
    if "resolution_minutes" not in frame.columns:
        if "resolution" not in frame.columns:
            raise ValueError("Input price series must include either `resolution_minutes` or `resolution`")
        resolution_mapping = {"PT15M": 15, "PT30M": 30, "PT60M": 60, "PT1H": 60}
        frame["resolution_minutes"] = frame["resolution"].map(resolution_mapping)
    if "value_kind" not in frame.columns:
        if "is_forecast" in frame.columns:
            frame["value_kind"] = frame["is_forecast"].map(lambda is_forecast: "forecast" if is_forecast else "actual")
        else:
            frame["value_kind"] = value_kind
    frame["timestamp_utc"] = pd.to_datetime(frame["timestamp_utc"], utc=True, format="mixed")
    frame["timestamp_local"] = pd.to_datetime(frame["timestamp_local"], utc=True, format="mixed").dt.tz_convert(
        timezone
    )
    frame = frame.sort_values("timestamp_utc").reset_index(drop=True)
    resolution = int(frame["resolution_minutes"].iloc[0])
    return PriceSeries(
        name=name,
        market=market,
        zone=zone,
        resolution_minutes=resolution,
        source=source,
        value_kind=value_kind,
        data=frame,
        metadata={"path": str(target)},
    )


def save_price_series(series: PriceSeries, path: str | Path) -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    suffix = target.suffix.lower()
    if suffix == ".parquet":
        series.data.to_parquet(target, index=False)
    elif suffix == ".csv":
        series.data.to_csv(target, index=False)
    elif suffix == ".json":
        with target.open("w", encoding="utf-8") as handle:
            json.dump({"metadata": series.metadata, "data": series.data.to_dict(orient="records")}, handle, indent=2)
    else:
        raise ValueError(f"Unsupported output format: {target}")
    return target


def _json_safe(value: Any) -> Any:
    if isinstance(value, dict):
        return {key: _json_safe(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_json_safe(item) for item in value]
    if isinstance(value, tuple):
        return [_json_safe(item) for item in value]
    if isinstance(value, float) and not math.isfinite(value):
        return None
    if value is None or isinstance(value, (str, int, bool)):
        return value
    try:
        if pd.isna(value):
            return None
    except Exception:
        pass
    return value


def save_json(payload: dict[str, Any], path: str | Path) -> Path:
    target = Path(path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", encoding="utf-8") as handle:
        json.dump(_json_safe(payload), handle, indent=2, sort_keys=True, allow_nan=False, default=str)
        handle.write("\n")
    return target
