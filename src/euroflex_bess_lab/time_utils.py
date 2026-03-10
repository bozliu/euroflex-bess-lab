from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

import pandas as pd

EUROPE_BRUSSELS = ZoneInfo("Europe/Brussels")
EUROPE_AMSTERDAM = ZoneInfo("Europe/Amsterdam")
UTC = ZoneInfo("UTC")


def ensure_utc_timestamp(value: pd.Timestamp | datetime | str) -> pd.Timestamp:
    timestamp = pd.Timestamp(value)
    if timestamp.tzinfo is None:
        timestamp = timestamp.tz_localize(UTC)
    return timestamp.tz_convert(UTC)


def to_local_timestamp(value: pd.Timestamp | datetime | str, timezone: str = "Europe/Brussels") -> pd.Timestamp:
    return ensure_utc_timestamp(value).tz_convert(ZoneInfo(timezone))


def resolution_code_to_minutes(code: str) -> int:
    mapping = {"PT15M": 15, "PT30M": 30, "PT60M": 60, "PT1H": 60}
    if code not in mapping:
        raise ValueError(f"Unsupported resolution code: {code}")
    return mapping[code]


def expand_to_resolution(
    frame: pd.DataFrame,
    source_resolution_minutes: int,
    target_resolution_minutes: int,
    *,
    timezone: str = "Europe/Brussels",
) -> pd.DataFrame:
    if source_resolution_minutes == target_resolution_minutes:
        return frame.copy()
    if source_resolution_minutes % target_resolution_minutes != 0:
        raise ValueError(
            "Target resolution must divide source resolution exactly: "
            f"{source_resolution_minutes=} {target_resolution_minutes=}"
        )

    repeat_count = source_resolution_minutes // target_resolution_minutes
    rows: list[dict[str, object]] = []
    for _, row in frame.iterrows():
        for offset in range(repeat_count):
            record = row.to_dict()
            record["timestamp_utc"] = ensure_utc_timestamp(row["timestamp_utc"]) + pd.Timedelta(
                minutes=offset * target_resolution_minutes
            )
            record["resolution_minutes"] = target_resolution_minutes
            record["provenance"] = f"expanded_from_{source_resolution_minutes}m"
            rows.append(record)

    expanded = pd.DataFrame(rows)
    if "timestamp_local" in expanded.columns:
        expanded["timestamp_local"] = expanded["timestamp_utc"].dt.tz_convert(ZoneInfo(timezone))
    return expanded
