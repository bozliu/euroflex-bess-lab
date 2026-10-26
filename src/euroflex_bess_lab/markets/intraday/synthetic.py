"""SYNTHETIC day-ahead and intraday prices for tests, demos and the no-licence fallback. Never market data.

The generator mimics the shapes that matter for the strategy: an evening peak and a solar midday dip in
day-ahead prices (hourly products before the 15-minute MTU, quarter-hourly after), and an intraday price equal to
day-ahead plus a persistent AR(1) spread and an intra-hour ramp shape (:class:`SyntheticIntradaySource`).
"""

from __future__ import annotations

from datetime import date

import numpy as np
import pandas as pd

from .config import LOCAL_TZ
from .data import IntradayMarket, SyntheticIntradaySource, local_midnight_utc

QUARTER_HOUR_MTU_START = date(2025, 10, 1)


def synthetic_day_ahead(
    first_day: date | str,
    n_days: int,
    *,
    hourly_until: date | str | None = QUARTER_HOUR_MTU_START,
    seed: int = 0,
) -> pd.DataFrame:
    """Quarter-hourly day-ahead frame (``timestamp_utc``, ``resolution_minutes``, ``price_eur_mwh``).

    Days before ``hourly_until`` carry hourly products (price constant over the hour, resolution 60).
    """

    rng = np.random.default_rng(seed)
    first = pd.Timestamp(first_day).date()
    last_exclusive = (pd.Timestamp(first) + pd.Timedelta(days=n_days)).date()
    stamps = pd.date_range(
        local_midnight_utc(first), local_midnight_utc(last_exclusive), freq="15min", inclusive="left"
    )
    local = stamps.tz_convert(LOCAL_TZ)
    hour = local.hour.to_numpy() + local.minute.to_numpy() / 60.0
    day_index = np.asarray((local.normalize().tz_localize(None) - pd.Timestamp(first)).days)
    level = 85.0 + rng.normal(0.0, 12.0, n_days + 1)[day_index]
    shape = 35.0 * np.cos(2 * np.pi * (hour - 19.0) / 24.0) - 40.0 * np.exp(-(((hour - 13.0) / 2.5) ** 2))
    price = level + shape + rng.normal(0.0, 6.0, len(stamps))
    cutoff = pd.Timestamp(hourly_until).date() if hourly_until is not None else None
    hourly = np.array([cutoff is not None and ts.date() < cutoff for ts in local])
    frame = pd.DataFrame({"timestamp_utc": stamps, "price_eur_mwh": price})
    frame["resolution_minutes"] = np.where(hourly, 60, 15).astype("int16")
    hour_key = frame["timestamp_utc"].dt.floor("h")
    hourly_mean = frame.groupby(hour_key)["price_eur_mwh"].transform("mean")
    frame.loc[hourly, "price_eur_mwh"] = hourly_mean[hourly]
    return frame[["timestamp_utc", "resolution_minutes", "price_eur_mwh"]]


def synthetic_market(
    first_day: date | str = "2026-01-01",
    n_days: int = 70,
    *,
    hourly_until: date | str | None = QUARTER_HOUR_MTU_START,
    seed: int = 0,
    phi: float = 0.85,
    sigma_eur_mwh: float = 18.0,
    ramp_eur_mwh: float = 12.0,
) -> IntradayMarket:
    """A complete synthetic :class:`IntradayMarket` (labelled ``synthetic``)."""

    da = synthetic_day_ahead(first_day, n_days, hourly_until=hourly_until, seed=seed)
    series = pd.Series(da["price_eur_mwh"].to_numpy(), index=pd.DatetimeIndex(da["timestamp_utc"]))
    source = SyntheticIntradaySource(
        series, phi=phi, sigma_eur_mwh=sigma_eur_mwh, ramp_eur_mwh=ramp_eur_mwh, seed=seed + 1
    )
    intraday = source.load(series.index[0], series.index[-1] + pd.Timedelta(minutes=15))
    return IntradayMarket.from_frames(da, intraday, id_source=source.name, id_execution_label=source.execution_label)
