from __future__ import annotations

import pandas as pd
import pytest

from euroflex_bess_lab.forecasts.providers import PersistenceForecastProvider

TZ = "Europe/Brussels"


def _local_day_frame(local_date: str, *, drop: int | None = None) -> pd.DataFrame:
    start = pd.Timestamp(local_date).tz_localize(TZ)
    end = (pd.Timestamp(local_date) + pd.Timedelta(days=1)).tz_localize(TZ)
    utc = pd.date_range(start.tz_convert("UTC"), end.tz_convert("UTC"), freq="15min", inclusive="left")
    frame = pd.DataFrame(
        {
            "timestamp_utc": utc,
            "timestamp_local": utc.tz_convert(TZ),
            "resolution_minutes": 15,
            "price_eur_per_mwh": [float(i) for i in range(len(utc))],
        }
    )
    if drop is not None:
        frame = frame.drop(index=drop).reset_index(drop=True)
    return frame


def _forecast(previous: pd.DataFrame, delivery: pd.DataFrame) -> pd.DataFrame:
    decision = delivery["timestamp_utc"].iloc[0] - pd.Timedelta(hours=12)
    return PersistenceForecastProvider().get_forecast(
        market="day_ahead", decision_time_utc=decision, delivery_frame=delivery, actual_frame=previous
    )


def test_persistence_after_spring_forward_day_fills_skipped_clock_times() -> None:
    previous = _local_day_frame("2026-03-29")  # 23-hour day: 02:00-02:45 local do not exist
    assert len(previous) == 92
    forecast = _forecast(previous, _local_day_frame("2026-03-30"))
    assert len(forecast) == 96
    values = forecast["forecast_price_eur_per_mwh"].tolist()
    assert values[8:12] == [7.0, 7.0, 7.0, 7.0]  # 02:00-02:45 take the 01:45 value of the short day
    assert values[12] == 8.0  # 03:00 maps to 03:00


def test_persistence_after_fall_back_day_uses_first_repeated_clock() -> None:
    previous = _local_day_frame("2026-10-25")  # 25-hour day: 02:00-02:45 local occur twice
    assert len(previous) == 100
    forecast = _forecast(previous, _local_day_frame("2026-10-26"))
    assert len(forecast) == 96
    assert forecast["forecast_price_eur_per_mwh"].iloc[8] == 8.0


def test_persistence_still_rejects_real_gaps() -> None:
    with pytest.raises(ValueError, match="full previous local day"):
        _forecast(_local_day_frame("2026-03-10", drop=40), _local_day_frame("2026-03-11"))
