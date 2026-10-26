"""Minute-grid market data and the no-look-ahead decision context."""

from __future__ import annotations

import importlib
import math
from collections.abc import Iterable, Mapping
from dataclasses import dataclass, field
from datetime import timedelta
from typing import Any

import numpy as np
import pandas as pd
from numpy.typing import NDArray

from .types import MINUTES_PER_QH, BatteryParams, PublicationModel

FloatArray = NDArray[np.float64]
IntArray = NDArray[np.int64]

DEFAULT_TIMEZONE = "Europe/Brussels"
NEW_FORMULA_PREFIX = "nf_"
SI_FORECAST_CURRENT_PREFIX = "elia_fc_cur_"
SI_FORECAST_NEXT_PREFIX = "elia_fc_next_"


def _utc(value: str | pd.Timestamp) -> pd.Timestamp:
    ts = pd.Timestamp(value)
    if ts.tzinfo is None:
        ts = ts.tz_localize("UTC")
    return ts.tz_convert("UTC")


def _check_qh_aligned(ts: pd.Timestamp, label: str) -> None:
    if ts.second or ts.microsecond or ts.nanosecond or ts.minute % MINUTES_PER_QH:
        raise ValueError(f"{label} must be aligned to a quarter-hour boundary, got {ts}")


def _numeric_arrays(
    frame: pd.DataFrame,
    grid: pd.DatetimeIndex,
    *,
    time_column: str,
    prefix: str = "",
    exclude: Iterable[str] = (),
) -> dict[str, FloatArray]:
    if time_column not in frame.columns:
        raise KeyError(f"frame is missing time column `{time_column}`")
    data = frame.copy()
    data[time_column] = pd.to_datetime(data[time_column], utc=True)
    data = data.drop_duplicates(subset=[time_column], keep="last").set_index(time_column).sort_index()
    data = data.reindex(grid)
    skip = set(exclude)
    arrays: dict[str, FloatArray] = {}
    for column in data.columns:
        if column in skip:
            continue
        series = data[column]
        if pd.api.types.is_bool_dtype(series) or pd.api.types.is_numeric_dtype(series):
            arrays[f"{prefix}{column}"] = series.to_numpy(dtype=float, na_value=np.nan)
    return arrays


def expand_day_ahead_to_qh(da_frame: pd.DataFrame, grid: pd.DatetimeIndex) -> FloatArray:
    """Expand hourly or 15-minute day-ahead rows (``timestamp_utc``, ``resolution_minutes``, ``price_eur_mwh``)."""
    if da_frame.empty:
        return np.full(len(grid), np.nan)
    frame = da_frame.copy()
    frame["timestamp_utc"] = pd.to_datetime(frame["timestamp_utc"], utc=True)
    if "resolution_minutes" not in frame.columns:
        frame["resolution_minutes"] = MINUTES_PER_QH
    steps = (frame["resolution_minutes"].astype(int) // MINUTES_PER_QH).clip(lower=1).to_numpy()
    starts = pd.DatetimeIndex(frame["timestamp_utc"]).repeat(steps)
    offsets = pd.to_timedelta(np.concatenate([np.arange(step) for step in steps]) * MINUTES_PER_QH, unit="m")
    prices = frame["price_eur_mwh"].to_numpy(dtype=float).repeat(steps)
    expanded = pd.Series(prices, index=starts + offsets)
    expanded = expanded[~expanded.index.duplicated(keep="last")]
    return expanded.reindex(grid).to_numpy(dtype=float, na_value=np.nan)


def day_ahead_visible_from_minute(
    qh_times: pd.DatetimeIndex,
    start_utc: pd.Timestamp,
    *,
    timezone: str,
    publication_hour_local: int,
) -> IntArray:
    """Minute index (relative to ``start_utc``) from which each QH's day-ahead price is public."""
    local = qh_times.tz_convert(timezone)
    day_start = local.normalize()
    published_local = day_start - pd.Timedelta(days=1) + pd.Timedelta(hours=publication_hour_local)
    published_utc = published_local.tz_convert("UTC")
    minutes = (published_utc - start_utc) / pd.Timedelta(minutes=1)
    return np.maximum(np.ceil(np.asarray(minutes, dtype=float)), 0).astype(np.int64)


@dataclass
class MarketData:
    """Aligned numpy arrays on a minute grid and a quarter-hour grid.

    ``minute`` and ``qh`` hold numeric fields from the public data contract; ``settlement_prices`` maps a
    mechanism name (``current``, ``new2026``, ...) to the quarter-hour settlement price used for P&L.
    """

    start_utc: pd.Timestamp
    end_utc: pd.Timestamp
    minute_times: pd.DatetimeIndex
    qh_times: pd.DatetimeIndex
    minute: dict[str, FloatArray]
    qh: dict[str, FloatArray]
    settlement_prices: dict[str, FloatArray]
    da_price: FloatArray
    da_visible_from_minute: IntArray
    timezone: str = DEFAULT_TIMEZONE
    meta: dict[str, Any] = field(default_factory=dict)

    @property
    def n_minutes(self) -> int:
        return len(self.minute_times)

    @property
    def n_qh(self) -> int:
        return len(self.qh_times)

    @property
    def days(self) -> float:
        return self.n_minutes / 1440.0

    def require_minute(self, name: str) -> FloatArray:
        if name not in self.minute:
            raise KeyError(f"minute field `{name}` not loaded; available: {sorted(self.minute)}")
        return self.minute[name]

    def require_qh(self, name: str) -> FloatArray:
        if name not in self.qh:
            raise KeyError(f"quarter-hour field `{name}` not loaded; available: {sorted(self.qh)}")
        return self.qh[name]

    def settlement(self, mechanism: str) -> FloatArray:
        if mechanism not in self.settlement_prices:
            raise KeyError(
                f"settlement price `{mechanism}` not available for this period; "
                f"available: {sorted(self.settlement_prices)}"
            )
        return self.settlement_prices[mechanism]

    def qh_frame(self) -> pd.DataFrame:
        frame = pd.DataFrame(self.qh, index=self.qh_times)
        for name, prices in self.settlement_prices.items():
            frame[f"settlement_{name}_eur_mwh"] = prices
        frame["da_price_eur_mwh"] = self.da_price
        frame.index.name = "timestamp_utc"
        return frame.reset_index()

    def slice(self, start: str | pd.Timestamp, end: str | pd.Timestamp) -> MarketData:
        """Sub-period view (quarter-hour aligned) with re-based day-ahead visibility."""
        start_ts, end_ts = _utc(start), _utc(end)
        _check_qh_aligned(start_ts, "start")
        _check_qh_aligned(end_ts, "end")
        m0 = int((start_ts - self.start_utc) / pd.Timedelta(minutes=1))
        m1 = int((end_ts - self.start_utc) / pd.Timedelta(minutes=1))
        if not 0 <= m0 < m1 <= self.n_minutes:
            raise ValueError("slice is outside the loaded period")
        q0, q1 = m0 // MINUTES_PER_QH, m1 // MINUTES_PER_QH
        return MarketData(
            start_utc=start_ts,
            end_utc=end_ts,
            minute_times=self.minute_times[m0:m1],
            qh_times=self.qh_times[q0:q1],
            minute={k: v[m0:m1] for k, v in self.minute.items()},
            qh={k: v[q0:q1] for k, v in self.qh.items()},
            settlement_prices={k: v[q0:q1] for k, v in self.settlement_prices.items()},
            da_price=self.da_price[q0:q1],
            da_visible_from_minute=np.maximum(self.da_visible_from_minute[q0:q1] - m0, 0),
            timezone=self.timezone,
            meta=dict(self.meta),
        )

    def with_minute_fields(self, fields: Mapping[str, FloatArray]) -> MarketData:
        """Copy with extra (derived) minute fields, e.g. strategy features."""
        merged = dict(self.minute)
        for name, values in fields.items():
            if len(values) != self.n_minutes:
                raise ValueError(f"minute field `{name}` has the wrong length")
            merged[name] = np.asarray(values, dtype=float)
        return MarketData(**{**self.__dict__, "minute": merged})

    @classmethod
    def from_frames(
        cls,
        *,
        start: str | pd.Timestamp,
        end: str | pd.Timestamp,
        minute_frames: Mapping[str, pd.DataFrame] | None = None,
        qh_frames: Mapping[str, pd.DataFrame] | None = None,
        settlement_columns: Mapping[str, str] | None = None,
        da_frame: pd.DataFrame | None = None,
        si_forecast_frames: Mapping[str, pd.DataFrame] | None = None,
        timezone: str = DEFAULT_TIMEZONE,
        publication: PublicationModel | None = None,
        meta: Mapping[str, Any] | None = None,
    ) -> MarketData:
        """Build from contract-shaped frames.

        ``minute_frames`` / ``qh_frames`` map a column prefix (``""`` for the core tables, ``"nf_"`` for the
        new-formula tables) to a frame with a ``timestamp_utc`` column. ``settlement_columns`` maps a mechanism name
        to a (prefixed) quarter-hour column; by default ``current`` <- ``imbalance_price_eur_mwh`` and
        ``new2026`` <- ``nf_imbalance_price_eur_mwh`` when present. ``si_forecast_frames`` maps
        ``current_qh`` / ``next_qh`` to Elia SI-forecast frames, aligned on their prediction minute.
        """
        start_ts, end_ts = _utc(start), _utc(end)
        _check_qh_aligned(start_ts, "start")
        _check_qh_aligned(end_ts, "end")
        if end_ts <= start_ts:
            raise ValueError("end must be after start")
        minute_times = pd.date_range(start_ts, end_ts, freq="1min", inclusive="left")
        qh_times = pd.date_range(start_ts, end_ts, freq="15min", inclusive="left")
        minute: dict[str, FloatArray] = {}
        for prefix, frame in (minute_frames or {}).items():
            minute.update(_numeric_arrays(frame, minute_times, time_column="timestamp_utc", prefix=prefix))
        for kind, frame in (si_forecast_frames or {}).items():
            prefix = SI_FORECAST_CURRENT_PREFIX if kind == "current_qh" else SI_FORECAST_NEXT_PREFIX
            aligned = frame.copy()
            aligned["timestamp_utc"] = pd.to_datetime(aligned["prediction_time_utc"], utc=True).dt.floor("1min")
            minute.update(
                _numeric_arrays(
                    aligned,
                    minute_times,
                    time_column="timestamp_utc",
                    prefix=prefix,
                    exclude=("prediction_time_utc", "target_qh_utc"),
                )
            )
        qh: dict[str, FloatArray] = {}
        for prefix, frame in (qh_frames or {}).items():
            qh.update(_numeric_arrays(frame, qh_times, time_column="timestamp_utc", prefix=prefix))
        mapping = dict(settlement_columns or {})
        if not mapping:
            if "imbalance_price_eur_mwh" in qh:
                mapping["current"] = "imbalance_price_eur_mwh"
            if f"{NEW_FORMULA_PREFIX}imbalance_price_eur_mwh" in qh:
                mapping["new2026"] = f"{NEW_FORMULA_PREFIX}imbalance_price_eur_mwh"
        settlement_prices = {name: qh[column] for name, column in mapping.items() if column in qh}
        publication = publication or PublicationModel()
        da_price = (
            expand_day_ahead_to_qh(da_frame, qh_times) if da_frame is not None else np.full(len(qh_times), np.nan)
        )
        da_visible = day_ahead_visible_from_minute(
            qh_times,
            start_ts,
            timezone=timezone,
            publication_hour_local=publication.da_publication_hour_local,
        )
        return cls(
            start_utc=start_ts,
            end_utc=end_ts,
            minute_times=minute_times,
            qh_times=qh_times,
            minute=minute,
            qh=qh,
            settlement_prices=settlement_prices,
            da_price=da_price,
            da_visible_from_minute=da_visible,
            timezone=timezone,
            meta=dict(meta or {}),
        )

    @classmethod
    def from_public(
        cls,
        start: str | pd.Timestamp,
        end: str | pd.Timestamp,
        *,
        include_new_formula: bool | None = None,
        include_si_forecast: bool = True,
        include_day_ahead: bool = True,
        minute_columns: list[str] | None = None,
        publication: PublicationModel | None = None,
    ) -> MarketData:
        """Load Belgian real-time data from the committed public data set.

        The data root is ``data/public`` (nearest one walking up from the working directory) or
        ``$EUROFLEX_DATA_DIR``.
        """
        pdata: Any = importlib.import_module("euroflex_bess_lab.public_data")  # lazy: optional at import time

        start_ts, end_ts = _utc(start), _utc(end)
        kwargs: dict[str, Any] = {}
        minute_frames: dict[str, pd.DataFrame] = {
            "": pdata.load_belgium_minute(start_ts, end_ts, columns=minute_columns, **kwargs)
        }
        qh_frames: dict[str, pd.DataFrame] = {"": pdata.load_belgium_qh(start_ts, end_ts, **kwargs)}
        new_formula_window = (_utc("2026-01-01"), _utc("2026-08-01"))
        overlaps = start_ts < new_formula_window[1] and end_ts > new_formula_window[0]
        if include_new_formula is None:
            include_new_formula = overlaps
        if include_new_formula:
            minute_frames[NEW_FORMULA_PREFIX] = pdata.load_belgium_new_formula_minute(start_ts, end_ts, **kwargs)
            qh_frames[NEW_FORMULA_PREFIX] = pdata.load_belgium_new_formula_qh(start_ts, end_ts, **kwargs)
        si_frames: dict[str, pd.DataFrame] = {}
        if include_si_forecast:
            for kind in ("current_qh", "next_qh"):
                try:
                    si_frames[kind] = pdata.load_belgium_si_forecast(kind, start_ts, end_ts, **kwargs)
                except FileNotFoundError:
                    continue
        da_frame = None
        if include_day_ahead:
            try:
                da_frame = pdata.load_day_ahead_prices("BE", start_ts, end_ts, **kwargs)
            except FileNotFoundError:
                da_frame = None
        return cls.from_frames(
            start=start_ts,
            end=end_ts,
            minute_frames=minute_frames,
            qh_frames=qh_frames,
            da_frame=da_frame,
            si_forecast_frames=si_frames,
            publication=publication,
            meta={"source": "data/public", "include_new_formula": include_new_formula},
        )


class DecisionContext:
    """What a strategy may see when deciding the power for minute ``t``.

    Use the helper methods: they only return information that the :class:`PublicationModel` makes visible.
    Reading ``market`` arrays directly is allowed only for indices below ``visible_minute_end`` (minute fields)
    or ``visible_qh_end`` (quarter-hour fields and settlement prices); :func:`~euroflex_bess_lab.realtime.audit`
    checks this by perturbing the future.
    """

    __slots__ = (
        "market",
        "battery",
        "publication",
        "mechanism",
        "t",
        "qh_index",
        "minute_in_qh",
        "soc_mwh",
        "visible_minute_end",
        "visible_qh_end",
        "last_power_mw",
    )

    def __init__(self, market: MarketData, battery: BatteryParams, publication: PublicationModel, mechanism: str):
        self.market = market
        self.battery = battery
        self.publication = publication
        self.mechanism = mechanism
        self.t = 0
        self.qh_index = 0
        self.minute_in_qh = 0
        self.soc_mwh = battery.initial_soc_mwh
        self.visible_minute_end = 0
        self.visible_qh_end = 0
        self.last_power_mw = 0.0

    def advance(self, t: int, soc_mwh: float) -> None:
        self.t = t
        self.qh_index = t // MINUTES_PER_QH
        self.minute_in_qh = t - self.qh_index * MINUTES_PER_QH
        self.soc_mwh = soc_mwh
        self.visible_minute_end = self.publication.visible_minute_end(t)
        self.visible_qh_end = self.publication.visible_qh_end(t)

    @property
    def now_utc(self) -> pd.Timestamp:
        return self.market.start_utc + timedelta(minutes=self.t)

    @property
    def qh_start_minute(self) -> int:
        return self.qh_index * MINUTES_PER_QH

    @property
    def soc_frac(self) -> float:
        return self.soc_mwh / self.battery.energy_mwh

    def latest(self, name: str) -> float:
        """Most recent visible value of a minute field (may belong to a previous quarter-hour)."""
        end = self.visible_minute_end
        if end <= 0:
            return math.nan
        return float(self.market.require_minute(name)[end - 1])

    def latest_in_current_qh(self, name: str) -> float:
        """Most recent visible value of a minute field inside the current quarter-hour, else NaN."""
        end = self.visible_minute_end
        if end - 1 < self.qh_start_minute or end <= 0:
            return math.nan
        return float(self.market.require_minute(name)[end - 1])

    def visible_minutes_in_current_qh(self) -> int:
        return max(0, self.visible_minute_end - self.qh_start_minute)

    def minute_window(self, name: str, n: int) -> FloatArray:
        end = self.visible_minute_end
        return self.market.require_minute(name)[max(0, end - n) : end]

    def qh_window(self, name: str, n: int) -> FloatArray:
        end = self.visible_qh_end
        return self.market.require_qh(name)[max(0, end - n) : end]

    def settlement_window(self, n: int, mechanism: str | None = None) -> FloatArray:
        end = self.visible_qh_end
        return self.market.settlement(mechanism or self.mechanism)[max(0, end - n) : end]

    def da_price(self, qh_index: int) -> float:
        if not 0 <= qh_index < self.market.n_qh:
            return math.nan
        if self.t < int(self.market.da_visible_from_minute[qh_index]):
            return math.nan
        return float(self.market.da_price[qh_index])

    def da_prices_ahead(self, n: int) -> FloatArray:
        k0 = self.qh_index
        k1 = min(self.market.n_qh, k0 + n)
        values = self.market.da_price[k0:k1].copy()
        hidden = self.market.da_visible_from_minute[k0:k1] > self.t
        values[hidden] = np.nan
        return values
