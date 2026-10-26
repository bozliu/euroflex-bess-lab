"""GB market time conventions: settlement days and periods (Europe/London) and EFA days and blocks.

* A **settlement day** is a Europe/London calendar day; its settlement periods (SPs) are consecutive 30-minute
  intervals starting at local midnight, numbered from 1 (46 SPs on the spring clock-change day, 50 in autumn).
* An **EFA day** starts at 23:00 Europe/London on the previous calendar day and is split into six 4-hour EFA
  blocks (23-03, 03-07, 07-11, 11-15, 15-19, 19-23 local; the block containing a clock change lasts 3 or 5 hours).
  The EFA date is the calendar date of the day that starts one hour after the EFA day begins.

Everything is vectorized over UTC timestamps; intervals are half-open ``[start, end)``.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

UK_TZ = "Europe/London"
SP_MINUTES = 30
SP_HOURS = SP_MINUTES / 60.0
EFA_BLOCK_HOURS = 4


def to_utc(value: object) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    return ts.tz_convert("UTC")


def uk_midnight_utc(day: object) -> pd.Timestamp:
    """UTC instant of Europe/London midnight starting the local calendar ``day`` (``YYYY-MM-DD``)."""

    local = pd.Timestamp(str(pd.Timestamp(day).date())).tz_localize(UK_TZ)
    return local.tz_convert("UTC")


def latest_uk_midnight_utc(at: pd.Timestamp | None = None) -> pd.Timestamp:
    """The latest Europe/London midnight (as UTC) at or before ``at`` (default: now). Used as the snapshot end."""

    moment = (at if at is not None else pd.Timestamp.now(tz="UTC")).tz_convert(UK_TZ)
    return moment.normalize().tz_convert("UTC")


def sp_grid(start: object, end: object) -> pd.DatetimeIndex:
    """UTC starts of all settlement periods in ``[start, end)`` (both aligned to 30 minutes)."""

    lo, hi = to_utc(start), to_utc(end)
    if lo.minute % SP_MINUTES or lo.second or hi.minute % SP_MINUTES or hi.second:
        raise ValueError("start and end must be aligned to settlement periods (hh:00 or hh:30)")
    return pd.date_range(lo, hi, freq=f"{SP_MINUTES}min", inclusive="left", tz="UTC")


def settlement_date_period(timestamps: pd.DatetimeIndex | pd.Series) -> tuple[np.ndarray, np.ndarray]:
    """``(settlement_date as YYYY-MM-DD strings, settlement_period as int)`` for SP start timestamps (UTC)."""

    ts = pd.DatetimeIndex(pd.to_datetime(timestamps, utc=True))
    local = ts.tz_convert(UK_TZ)
    local_dates = local.normalize()
    midnight_utc = local_dates.tz_convert("UTC")
    # ``normalize`` on a tz-aware index keeps the local wall-clock midnight, so its UTC value is the period origin.
    period = ((ts - midnight_utc) / pd.Timedelta(minutes=SP_MINUTES)).astype(np.int64) + 1
    dates = np.asarray(local.strftime("%Y-%m-%d"), dtype=object)
    return dates, np.asarray(period, dtype=np.int16)


def settlement_grid(start: object, end: object) -> pd.DataFrame:
    """Frame with ``timestamp_utc``, ``settlement_date``, ``settlement_period`` for every SP in ``[start, end)``."""

    index = sp_grid(start, end)
    dates, periods = settlement_date_period(index)
    return pd.DataFrame({"timestamp_utc": index, "settlement_date": dates, "settlement_period": periods})


def sp_start_from_date_period(settlement_date: object, settlement_period: int) -> pd.Timestamp:
    """UTC start of settlement period ``settlement_period`` (1-based) on ``settlement_date``."""

    if settlement_period < 1 or settlement_period > 50:
        raise ValueError("settlement_period must be between 1 and 50")
    return uk_midnight_utc(settlement_date) + pd.Timedelta(minutes=SP_MINUTES * (settlement_period - 1))


def efa_date_block(timestamps: pd.DatetimeIndex | pd.Series) -> tuple[np.ndarray, np.ndarray]:
    """``(EFA date as YYYY-MM-DD strings, EFA block 1..6)`` for UTC timestamps."""

    ts = pd.DatetimeIndex(pd.to_datetime(timestamps, utc=True))
    shifted = ts.tz_convert(UK_TZ) + pd.Timedelta(hours=1)
    # Wall-clock shift: EFA day D runs 23:00 (D-1) .. 23:00 (D) local, i.e. 00:00 .. 24:00 after adding one hour.
    hours = np.asarray(shifted.hour, dtype=np.int64)
    blocks = hours // EFA_BLOCK_HOURS + 1
    dates = np.asarray(shifted.strftime("%Y-%m-%d"), dtype=object)
    return dates, np.asarray(blocks, dtype=np.int8)


def efa_day_start_utc(efa_date: object) -> pd.Timestamp:
    """UTC start of EFA day ``efa_date`` (23:00 Europe/London on the previous calendar day)."""

    day = pd.Timestamp(str(pd.Timestamp(efa_date).date()))
    local = (day - pd.Timedelta(days=1) + pd.Timedelta(hours=23)).tz_localize(UK_TZ)
    return local.tz_convert("UTC")


def eac_gate_closure_utc(efa_date: object, hour_local: int = 14) -> pd.Timestamp:
    """EAC response/reserve auction gate closure: 14:00 Europe/London on the day before ``efa_date``."""

    day = pd.Timestamp(str(pd.Timestamp(efa_date).date()))
    local = (day - pd.Timedelta(days=1) + pd.Timedelta(hours=hour_local)).tz_localize(UK_TZ)
    return local.tz_convert("UTC")
