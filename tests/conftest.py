from __future__ import annotations

import shutil
from pathlib import Path

import pandas as pd
import pytest


def repo_root() -> Path:
    return Path(__file__).resolve().parents[1]


@pytest.fixture(autouse=True)
def clean_generated_repo_outputs() -> None:
    generated = repo_root() / "artifacts" / "examples"
    shutil.rmtree(generated, ignore_errors=True)
    yield
    shutil.rmtree(generated, ignore_errors=True)


def _build_market_frame(
    *,
    start_utc: str,
    prices: list[float],
    market: str,
    source: str,
    zone: str,
    timezone: str,
    extra_columns: dict[str, list[object]] | None = None,
) -> pd.DataFrame:
    index = pd.date_range(start_utc, periods=len(prices), freq="15min", tz="UTC")
    frame = pd.DataFrame(
        {
            "timestamp_utc": index,
            "timestamp_local": index.tz_convert(timezone),
            "market": market,
            "resolution_minutes": 15,
            "price_eur_per_mwh": prices,
            "currency": "EUR",
            "zone": zone,
            "source": source,
            "value_kind": "actual",
            "provenance": "unit_test_fixture",
        }
    )
    if extra_columns:
        for key, values in extra_columns.items():
            frame[key] = values
    return frame


def _write_frame(path: Path, frame: pd.DataFrame) -> Path:
    path.parent.mkdir(parents=True, exist_ok=True)
    frame.to_csv(path, index=False)
    return path


def _build_afrr_frames(
    *,
    start_utc: str,
    timezone: str,
    zone: str,
    source_prefix: str,
    capacity_up: list[float],
    capacity_down: list[float],
    activation_up: list[float],
    activation_down: list[float],
    ratio_up: list[float],
    ratio_down: list[float],
) -> dict[str, pd.DataFrame]:
    return {
        "afrr_capacity_up": _build_market_frame(
            start_utc=start_utc,
            prices=capacity_up,
            market="afrr_capacity_up",
            source=f"{source_prefix}_capacity_up",
            zone=zone,
            timezone=timezone,
        ),
        "afrr_capacity_down": _build_market_frame(
            start_utc=start_utc,
            prices=capacity_down,
            market="afrr_capacity_down",
            source=f"{source_prefix}_capacity_down",
            zone=zone,
            timezone=timezone,
        ),
        "afrr_activation_price_up": _build_market_frame(
            start_utc=start_utc,
            prices=activation_up,
            market="afrr_activation_price_up",
            source=f"{source_prefix}_activation_up",
            zone=zone,
            timezone=timezone,
        ),
        "afrr_activation_price_down": _build_market_frame(
            start_utc=start_utc,
            prices=activation_down,
            market="afrr_activation_price_down",
            source=f"{source_prefix}_activation_down",
            zone=zone,
            timezone=timezone,
        ),
        "afrr_activation_ratio_up": _build_market_frame(
            start_utc=start_utc,
            prices=ratio_up,
            market="afrr_activation_ratio_up",
            source=f"{source_prefix}_ratio_up",
            zone=zone,
            timezone=timezone,
        ),
        "afrr_activation_ratio_down": _build_market_frame(
            start_utc=start_utc,
            prices=ratio_down,
            market="afrr_activation_ratio_down",
            source=f"{source_prefix}_ratio_down",
            zone=zone,
            timezone=timezone,
        ),
    }


@pytest.fixture
def two_day_market_data(tmp_path: Path) -> dict[str, Path]:
    first_day_da = [32.0] * 16 + [18.0] * 16 + [5.0] * 16 + [45.0] * 16 + [90.0] * 16 + [55.0] * 16
    second_day_da = [36.0] * 16 + [22.0] * 16 + [8.0] * 16 + [60.0] * 16 + [96.0] * 16 + [58.0] * 16
    day_ahead = _build_market_frame(
        start_utc="2025-06-15T22:00:00Z",
        prices=first_day_da + second_day_da,
        market="day_ahead",
        source="unit_test_day_ahead",
        zone="10YBE----------2",
        timezone="Europe/Brussels",
    )
    first_day_imb = [20.0, 18.0, 16.0, 14.0] * 24
    second_day_imb = [25.0, 10.0, 35.0, 15.0] * 24
    imbalance = _build_market_frame(
        start_utc="2025-06-15T22:00:00Z",
        prices=first_day_imb + second_day_imb,
        market="imbalance",
        source="unit_test_imbalance",
        zone="10YBE----------2",
        timezone="Europe/Brussels",
    )
    first_day_fcr = [18.0] * 32 + [26.0] * 32 + [22.0] * 32
    second_day_fcr = [17.0] * 32 + [28.0] * 32 + [23.0] * 32
    fcr_capacity = _build_market_frame(
        start_utc="2025-06-15T22:00:00Z",
        prices=first_day_fcr + second_day_fcr,
        market="fcr_capacity",
        source="unit_test_fcr_capacity",
        zone="10YBE----------2",
        timezone="Europe/Brussels",
    )
    afrr_frames = _build_afrr_frames(
        start_utc="2025-06-15T22:00:00Z",
        timezone="Europe/Brussels",
        zone="10YBE----------2",
        source_prefix="unit_test_afrr_be",
        capacity_up=([35.0] * 24 + [28.0] * 24 + [18.0] * 24 + [42.0] * 24) * 2,
        capacity_down=([12.0] * 24 + [16.0] * 24 + [38.0] * 24 + [14.0] * 24) * 2,
        activation_up=([110.0] * 24 + [95.0] * 24 + [65.0] * 24 + [145.0] * 24) * 2,
        activation_down=([45.0] * 24 + [55.0] * 24 + [92.0] * 24 + [40.0] * 24) * 2,
        ratio_up=([0.35] * 24 + [0.25] * 24 + [0.08] * 24 + [0.45] * 24) * 2,
        ratio_down=([0.05] * 24 + [0.08] * 24 + [0.28] * 24 + [0.06] * 24) * 2,
    )
    paths = {
        "day_ahead": _write_frame(tmp_path / "belgium_day_ahead.csv", day_ahead),
        "imbalance": _write_frame(tmp_path / "belgium_imbalance.csv", imbalance),
        "fcr_capacity": _write_frame(tmp_path / "belgium_fcr_capacity.csv", fcr_capacity),
    }
    for market_name, frame in afrr_frames.items():
        paths[market_name] = _write_frame(tmp_path / f"belgium_{market_name}.csv", frame)
    return paths


@pytest.fixture
def two_day_market_data_nl(tmp_path: Path) -> dict[str, Path]:
    first_day_da = [42.0] * 16 + [28.0] * 16 + [-4.0] * 16 + [31.0] * 16 + [110.0] * 16 + [62.0] * 16
    second_day_da = [46.0] * 16 + [30.0] * 16 + [-2.0] * 16 + [36.0] * 16 + [118.0] * 16 + [66.0] * 16
    day_ahead = _build_market_frame(
        start_utc="2025-06-15T22:00:00Z",
        prices=first_day_da + second_day_da,
        market="day_ahead",
        source="unit_test_day_ahead_nl",
        zone="10YNL----------L",
        timezone="Europe/Amsterdam",
    )
    shortage = ([85.0, 92.0, 120.0, 96.0] * 48)[:192]
    surplus = ([58.0, 64.0, 72.0, 60.0] * 48)[:192]
    midpoint = [(a + b) / 2.0 for a, b in zip(shortage, surplus, strict=True)]
    imbalance = _build_market_frame(
        start_utc="2025-06-15T22:00:00Z",
        prices=midpoint,
        market="imbalance",
        source="unit_test_imbalance_nl",
        zone="10YNL----------L",
        timezone="Europe/Amsterdam",
        extra_columns={
            "imbalance_shortage_price_eur_per_mwh": shortage,
            "imbalance_surplus_price_eur_per_mwh": surplus,
            "dispatch_up_price_eur_per_mwh": shortage,
            "dispatch_down_price_eur_per_mwh": surplus,
            "regulation_state": ([1, 1, -1, 0] * 48)[:192],
            "regulating_condition": (["UP", "UP", "DOWN", "NEUTRAL"] * 48)[:192],
        },
    )
    first_day_fcr = [16.0] * 24 + [20.0] * 24 + [24.0] * 24 + [18.0] * 24
    second_day_fcr = [15.0] * 24 + [19.0] * 24 + [26.0] * 24 + [17.0] * 24
    fcr_capacity = _build_market_frame(
        start_utc="2025-06-15T22:00:00Z",
        prices=first_day_fcr + second_day_fcr,
        market="fcr_capacity",
        source="unit_test_fcr_capacity_nl",
        zone="10YNL----------L",
        timezone="Europe/Amsterdam",
    )
    afrr_frames = _build_afrr_frames(
        start_utc="2025-06-15T22:00:00Z",
        timezone="Europe/Amsterdam",
        zone="10YNL----------L",
        source_prefix="unit_test_afrr_nl",
        capacity_up=([26.0] * 24 + [19.0] * 24 + [14.0] * 24 + [30.0] * 24) * 2,
        capacity_down=([9.0] * 24 + [11.0] * 24 + [20.0] * 24 + [10.0] * 24) * 2,
        activation_up=([105.0] * 24 + [88.0] * 24 + [60.0] * 24 + [122.0] * 24) * 2,
        activation_down=([40.0] * 24 + [47.0] * 24 + [70.0] * 24 + [42.0] * 24) * 2,
        ratio_up=([0.22] * 24 + [0.18] * 24 + [0.06] * 24 + [0.29] * 24) * 2,
        ratio_down=([0.04] * 24 + [0.06] * 24 + [0.13] * 24 + [0.05] * 24) * 2,
    )
    paths = {
        "day_ahead": _write_frame(tmp_path / "netherlands_day_ahead.csv", day_ahead),
        "imbalance": _write_frame(tmp_path / "netherlands_imbalance.csv", imbalance),
        "fcr_capacity": _write_frame(tmp_path / "netherlands_fcr_capacity.csv", fcr_capacity),
    }
    for market_name, frame in afrr_frames.items():
        paths[market_name] = _write_frame(tmp_path / f"netherlands_{market_name}.csv", frame)
    return paths


def build_csv_forecast(
    *,
    delivery_frame: pd.DataFrame,
    decision_time_utc: pd.Timestamp,
    market: str,
    provider_name: str = "csv",
    price_shift: float = 0.0,
    scenario_id: str | None = None,
) -> pd.DataFrame:
    resolution_minutes = int(delivery_frame["resolution_minutes"].iloc[0])
    snapshot = pd.DataFrame(
        {
            "market": market,
            "delivery_start_utc": delivery_frame["timestamp_utc"],
            "delivery_end_utc": delivery_frame["timestamp_utc"] + pd.Timedelta(minutes=resolution_minutes),
            "forecast_price_eur_per_mwh": delivery_frame["price_eur_per_mwh"] + price_shift,
            "issue_time_utc": decision_time_utc,
            "available_from_utc": decision_time_utc,
            "provider_name": provider_name,
            "scenario_id": scenario_id,
        }
    )
    return snapshot
