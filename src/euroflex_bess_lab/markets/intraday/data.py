"""Market data for the DE-LU two-stage backtest: day-ahead prices + a pluggable intraday execution price.

``IntradayPriceSource`` is the plug-in point. Shipped sources:

* :class:`IdAepIndexSource` - the TSOs' ID AEP from netztransparenz.de (built locally, not redistributed);
* :class:`FrameIntradaySource` - any quarter-hourly frame, e.g. licensed EPEX ID1 / ID3 indices or a VWAP you
  computed from licensed trade data (``timestamp_utc`` + a price column);
* :class:`SyntheticIntradaySource` - day-ahead plus an AR(1) spread and an intra-hour shape, for tests and demos
  only (never a market result).
"""

from __future__ import annotations

from dataclasses import dataclass, field
from datetime import date, datetime
from typing import Protocol, runtime_checkable

import numpy as np
import pandas as pd
from numpy.typing import NDArray

from .config import LOCAL_TZ

FloatArray = NDArray[np.float64]
IntArray = NDArray[np.int64]
type TimeLike = str | datetime | pd.Timestamp


def local_midnight_utc(day: date | pd.Timestamp | str) -> pd.Timestamp:
    return pd.Timestamp(day).normalize().tz_localize(LOCAL_TZ).tz_convert("UTC")


def _utc(value: TimeLike) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    return ts.tz_convert("UTC")


@runtime_checkable
class IntradayPriceSource(Protocol):
    """Quarter-hourly intraday execution price (EUR/MWh) on a UTC grid; NaN where the price is undefined."""

    name: str
    execution_label: str
    redistributable: bool

    def load(self, start_utc: pd.Timestamp, end_utc: pd.Timestamp) -> pd.Series: ...


@dataclass(frozen=True)
class IdAepIndexSource:
    """ID AEP (netztransparenz.de) as a VWAP-execution proxy for continuous intraday trades."""

    name: str = "id_aep"
    execution_label: str = "ID AEP (TSO index of the last 500 MW traded per quarter-hour; VWAP-execution proxy)"
    redistributable: bool = False

    def load(self, start_utc: pd.Timestamp, end_utc: pd.Timestamp) -> pd.Series:
        from ...public_data.intraday import ID_AEP_COLUMN, load_intraday_index

        frame = load_intraday_index(start_utc, end_utc)
        return pd.Series(frame[ID_AEP_COLUMN].to_numpy(dtype=float), index=pd.DatetimeIndex(frame["timestamp_utc"]))


@dataclass(frozen=True)
class FrameIntradaySource:
    """Intraday prices from a frame with ``timestamp_utc`` (quarter-hour start, UTC) and ``price_column``."""

    frame: pd.DataFrame
    price_column: str = "price_eur_mwh"
    name: str = "frame"
    execution_label: str = "user-supplied quarter-hourly intraday price"
    redistributable: bool = False

    def load(self, start_utc: pd.Timestamp, end_utc: pd.Timestamp) -> pd.Series:
        if "timestamp_utc" not in self.frame.columns or self.price_column not in self.frame.columns:
            raise KeyError(f"frame needs columns 'timestamp_utc' and {self.price_column!r}")
        stamps = pd.DatetimeIndex(pd.to_datetime(self.frame["timestamp_utc"], utc=True))
        series = pd.Series(self.frame[self.price_column].to_numpy(dtype=float), index=stamps).sort_index()
        series = series[~series.index.duplicated(keep="last")]
        return series[(series.index >= start_utc) & (series.index < end_utc)]


@dataclass(frozen=True)
class SyntheticIntradaySource:
    """Synthetic intraday prices = day-ahead + AR(1) spread + intra-hour ramp shape. Tests and demos only."""

    day_ahead: pd.Series
    phi: float = 0.8
    sigma_eur_mwh: float = 20.0
    ramp_eur_mwh: float = 10.0
    seed: int = 7
    name: str = "synthetic"
    execution_label: str = "SYNTHETIC intraday price (not market data)"
    redistributable: bool = True

    def load(self, start_utc: pd.Timestamp, end_utc: pd.Timestamp) -> pd.Series:
        da = self.day_ahead.sort_index()
        da = da[(da.index >= start_utc) & (da.index < end_utc)]
        rng = np.random.default_rng(self.seed)
        noise = rng.normal(0.0, self.sigma_eur_mwh, len(da))
        spread = np.zeros(len(da))
        for i in range(len(da)):
            spread[i] = (self.phi * spread[i - 1] if i else 0.0) + noise[i]
        quarter = (da.index.minute // 15).to_numpy()
        shape = self.ramp_eur_mwh * (quarter - 1.5) / 1.5
        return pd.Series(da.to_numpy(dtype=float) + spread + shape, index=da.index)


@dataclass
class IntradayMarket:
    """Day-ahead and intraday prices aligned on a complete UTC quarter-hour grid of whole local days."""

    timestamps: pd.DatetimeIndex
    da_price: FloatArray
    da_resolution_minutes: IntArray
    id_price: FloatArray
    slot: IntArray
    days: list[date]
    day_start: IntArray
    id_source: str = "frame"
    id_execution_label: str = ""
    zone: str = "DE-LU"
    _day_index: dict[date, int] = field(default_factory=dict, init=False, repr=False)

    def __post_init__(self) -> None:
        n = len(self.timestamps)
        for name in ("da_price", "da_resolution_minutes", "id_price", "slot"):
            if len(getattr(self, name)) != n:
                raise ValueError(f"{name} must have one value per quarter-hour ({n})")
        if len(self.day_start) != len(self.days) + 1:
            raise ValueError("day_start must have len(days) + 1 entries")
        self._day_index = {day: i for i, day in enumerate(self.days)}

    @property
    def n_qh(self) -> int:
        return len(self.timestamps)

    def day_bounds(self, day: date) -> tuple[int, int]:
        i = self._day_index[day]
        return int(self.day_start[i]), int(self.day_start[i + 1])

    def has_day(self, day: date) -> bool:
        return day in self._day_index

    def day_position(self, day: date) -> int:
        return self._day_index[day]

    def da_blocks(self, day: date) -> IntArray:
        """Day-ahead product id per quarter-hour of ``day`` (hourly products before the 15-minute MTU)."""

        lo, hi = self.day_bounds(day)
        resolution = self.da_resolution_minutes[lo:hi]
        stamps = self.timestamps[lo:hi]
        minutes = ((stamps - stamps[0]).total_seconds() // 60).to_numpy().astype(np.int64)
        block_len = np.maximum(resolution // 15, 1)
        keys = minutes // (block_len * 15)
        # Make block ids contiguous and unique per (resolution, start).
        key_pairs = list(zip(block_len.tolist(), keys.tolist(), strict=True))
        mapping: dict[tuple[int, int], int] = {}
        out = np.empty(len(key_pairs), dtype=np.int64)
        for i, pair in enumerate(key_pairs):
            out[i] = mapping.setdefault(pair, len(mapping))
        return out

    def with_id_prices(self, id_price: FloatArray, *, source: str | None = None) -> IntradayMarket:
        return IntradayMarket(
            timestamps=self.timestamps,
            da_price=self.da_price,
            da_resolution_minutes=self.da_resolution_minutes,
            id_price=np.asarray(id_price, dtype=float),
            slot=self.slot,
            days=list(self.days),
            day_start=self.day_start,
            id_source=source or self.id_source,
            id_execution_label=self.id_execution_label,
            zone=self.zone,
        )

    def with_da_prices(self, da_price: FloatArray) -> IntradayMarket:
        return IntradayMarket(
            timestamps=self.timestamps,
            da_price=np.asarray(da_price, dtype=float),
            da_resolution_minutes=self.da_resolution_minutes,
            id_price=self.id_price,
            slot=self.slot,
            days=list(self.days),
            day_start=self.day_start,
            id_source=self.id_source,
            id_execution_label=self.id_execution_label,
            zone=self.zone,
        )

    def slice_days(self, first: date, last: date) -> IntradayMarket:
        """Sub-market with local days ``[first, last]`` (inclusive)."""

        i0 = self._day_index[first]
        i1 = self._day_index[last] + 1
        lo, hi = int(self.day_start[i0]), int(self.day_start[i1])
        return IntradayMarket(
            timestamps=self.timestamps[lo:hi],
            da_price=self.da_price[lo:hi].copy(),
            da_resolution_minutes=self.da_resolution_minutes[lo:hi].copy(),
            id_price=self.id_price[lo:hi].copy(),
            slot=self.slot[lo:hi].copy(),
            days=list(self.days[i0:i1]),
            day_start=(self.day_start[i0 : i1 + 1] - lo).astype(np.int64),
            id_source=self.id_source,
            id_execution_label=self.id_execution_label,
            zone=self.zone,
        )

    @classmethod
    def from_frames(
        cls,
        day_ahead: pd.DataFrame,
        intraday: pd.Series | pd.DataFrame,
        *,
        first_day: date | str | None = None,
        last_day: date | str | None = None,
        id_price_column: str = "price_eur_mwh",
        id_source: str = "frame",
        id_execution_label: str = "",
    ) -> IntradayMarket:
        """Build from a quarter-hourly day-ahead frame (``timestamp_utc``, ``price_eur_mwh``,
        ``resolution_minutes``; hourly products already expanded to quarter-hours) and intraday prices.

        Only whole local days are kept; day-ahead must be complete on them, intraday may contain NaN.
        """

        da = day_ahead.copy()
        da["timestamp_utc"] = pd.to_datetime(da["timestamp_utc"], utc=True)
        da = da.drop_duplicates("timestamp_utc", keep="last").set_index("timestamp_utc").sort_index()
        if isinstance(intraday, pd.DataFrame):
            idf = intraday.copy()
            idf["timestamp_utc"] = pd.to_datetime(idf["timestamp_utc"], utc=True)
            id_series = idf.set_index("timestamp_utc")[id_price_column].astype(float)
        else:
            id_series = intraday.astype(float)
            id_series.index = pd.DatetimeIndex(pd.to_datetime(id_series.index, utc=True))
        id_series = id_series[~id_series.index.duplicated(keep="last")].sort_index()
        local_days = sorted({ts.date() for ts in da.index.tz_convert(LOCAL_TZ)})
        lo_day = pd.Timestamp(first_day).date() if first_day is not None else local_days[0]
        hi_day = pd.Timestamp(last_day).date() if last_day is not None else local_days[-1]
        stamps_parts: list[pd.DatetimeIndex] = []
        days: list[date] = []
        starts = [0]
        for day in pd.date_range(lo_day, hi_day, freq="D").date:
            grid = pd.date_range(
                local_midnight_utc(day), local_midnight_utc(day + pd.Timedelta(days=1)), freq="15min", inclusive="left"
            )
            if not grid.isin(da.index).all():
                continue  # incomplete day-ahead day: skipped (reported by the caller via `days`)
            stamps_parts.append(grid)
            days.append(day)
            starts.append(starts[-1] + len(grid))
        if not days:
            raise ValueError("no complete local day-ahead days in the requested range")
        stamps = stamps_parts[0].append(stamps_parts[1:]) if len(stamps_parts) > 1 else stamps_parts[0]
        da_rows = da.reindex(stamps)
        resolution = (
            da_rows["resolution_minutes"].to_numpy(dtype=np.int64)
            if "resolution_minutes" in da_rows.columns
            else np.full(len(stamps), 15, dtype=np.int64)
        )
        local = stamps.tz_convert(LOCAL_TZ)
        slot = (local.hour * 4 + local.minute // 15).to_numpy().astype(np.int64)
        return cls(
            timestamps=stamps,
            da_price=da_rows["price_eur_mwh"].to_numpy(dtype=float),
            da_resolution_minutes=resolution,
            id_price=id_series.reindex(stamps).to_numpy(dtype=float),
            slot=slot,
            days=days,
            day_start=np.asarray(starts, dtype=np.int64),
            id_source=id_source,
            id_execution_label=id_execution_label,
        )

    @classmethod
    def from_public(
        cls,
        first_day: date | str,
        last_day: date | str,
        *,
        source: IntradayPriceSource | None = None,
        history_days: int = 35,
    ) -> IntradayMarket:
        """Load DE-LU day-ahead prices (``data/public``, Energy-Charts, CC BY 4.0) and the intraday source.

        ``history_days`` extra local days before ``first_day`` are included for persistence and profile
        estimation (they are not evaluated by the backtest unless asked for).
        """

        from ...public_data import load_day_ahead_prices

        src = source or IdAepIndexSource()
        start_day = pd.Timestamp(first_day).date() - pd.Timedelta(days=history_days)
        end_day = pd.Timestamp(last_day).date()
        lo = local_midnight_utc(start_day)
        hi = local_midnight_utc(end_day + pd.Timedelta(days=1))
        da = load_day_ahead_prices("DE-LU", lo, hi, quarter_hourly=True)
        intraday = src.load(lo, hi)
        return cls.from_frames(
            da,
            intraday,
            first_day=start_day,
            last_day=end_day,
            id_source=src.name,
            id_execution_label=src.execution_label,
        )
