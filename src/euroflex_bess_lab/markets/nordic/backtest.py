"""Walk-forward Nordic BESS backtest: day-ahead arbitrage co-optimised with FCR-N/FCR-D, DK1 FCR, aFRR and mFRR
capacity, per bidding zone (DK1, DK2, SE3, SE4, FI), on the committed public data.

Causality. Delivery day D (CET/CEST) is decided once, at the earliest gate closure of the products in the
workflow (SE/DK2 FCR auction 1: D-1 00:30; aFRR/mFRR capacity: D-1 07:30; DK1 FCR: D-1 08:00; day-ahead: D-1
12:00). At every one of these gates the latest published prices are those of D-1 (D-1 day-ahead prices appear on
D-2 around 13:00, D-1 capacity results on D-2), so the persistence forecast (same local clock time on D-1, falling
back to D-2..D-7 for missing values) is identical at all gates and re-optimising at a later gate would reproduce
the same schedule. Every capacity product is pay-as-cleared, so offering at a price of zero is accepted and paid
the realised marginal price; the day-ahead schedule is submitted as a price-independent quantity bid. Settlement
uses realised prices. The oracle solves the same LP with realised prices (an upper bound, not tradable).
"""

from __future__ import annotations

from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from ...public_data.nordic import load_nordic_day_ahead, load_nordic_reserve
from ...realtime.types import QH_HOURS, BatteryParams
from .lp import DayInputs, DaySchedule, settle_day, solve_day
from .rules import PRODUCT_ORDER, ReserveProduct, zone_products

MARKET_TZ = "Europe/Copenhagen"
NORDIC_BATTERY = BatteryParams(
    power_mw=1.0,
    energy_mwh=2.0,
    charge_efficiency=0.95,
    discharge_efficiency=0.95,
    soc_min_frac=0.05,
    soc_max_frac=0.95,
    initial_soc_frac=0.5,
    degradation_eur_per_mwh=8.0,
)
WORKFLOWS: dict[str, tuple[str, ...] | None] = {
    "da_only": (),
    "da_fcr": ("fcr_n", "fcr_d_up", "fcr_d_down", "fcr_dk1"),
    "da_afrr": ("afrr_up", "afrr_down"),
    "da_mfrr": ("mfrr_up", "mfrr_down"),
    "full_stack": None,
}
WORKFLOW_LABELS = {
    "da_only": "DA only",
    "da_fcr": "DA + FCR",
    "da_afrr": "DA + aFRR",
    "da_mfrr": "DA + mFRR",
    "full_stack": "Full stack",
}
MODES = ("persistence", "oracle")
MAX_LOOKBACK_DAYS = 7
# Quarter-hours without a published day-ahead price are untradable; days missing more than this are skipped.
MAX_MISSING_DA_QH = 8
# Products whose missing hours mean "no procurement" (SVK publishes only hours with Swedish procurement).
MISSING_MEANS_ZERO = {("SE3", "mfrr_up"), ("SE3", "mfrr_down"), ("SE4", "mfrr_up"), ("SE4", "mfrr_down")}


def workflow_products(zone: str, workflow: str) -> list[ReserveProduct]:
    keys = WORKFLOWS[workflow]
    if keys == ():
        return []
    return zone_products(zone, keys)


def applicable_workflows(zone: str) -> list[str]:
    """Workflows that differ from DA-only in ``zone`` (e.g. Finland has no FCR/mFRR data)."""

    out = []
    for name in WORKFLOWS:
        if name in {"da_only", "full_stack"} or workflow_products(zone, name):
            out.append(name)
    return out


# --- market data ----------------------------------------------------------------------------------------------


def local_day_bounds(day: pd.Timestamp, tz: str = MARKET_TZ) -> tuple[pd.Timestamp, pd.Timestamp]:
    start = pd.Timestamp(day.date()).tz_localize(tz)
    end = (pd.Timestamp(day.date()) + pd.Timedelta(days=1)).tz_localize(tz)
    return start.tz_convert("UTC"), end.tz_convert("UTC")


def persistence_forecast(
    values: np.ndarray, index_utc: pd.DatetimeIndex, tz: str = MARKET_TZ, max_lookback_days: int = MAX_LOOKBACK_DAYS
) -> tuple[np.ndarray, np.ndarray]:
    """Value at the same local clock time on the previous day (D-1, else D-2 ... D-k) for every grid point.

    Returns ``(forecast, lag_days)``; ``lag_days`` is 0 where no previous value exists. Duplicate local times on
    the 25-hour day map to the first occurrence; local times that did not exist on the earlier day (23-hour day)
    are filled from the neighbouring slot of the same target day.
    """

    vals = np.asarray(values, dtype=np.float64)
    local = index_utc.tz_convert(tz).tz_localize(None)
    first = pd.Series(np.arange(len(local)), index=local)
    first = first[~first.index.duplicated(keep="first")]
    forecast = np.full(len(vals), np.nan)
    lag = np.zeros(len(vals), dtype=np.int64)
    for k in range(1, max_lookback_days + 1):
        pos = first.reindex(local - pd.Timedelta(days=k)).to_numpy()
        ok = np.isfinite(pos)
        cand = np.full(len(vals), np.nan)
        cand[ok] = vals[pos[ok].astype(np.int64)]
        fill = np.isnan(forecast) & np.isfinite(cand)
        forecast[fill] = cand[fill]
        lag[fill] = k
    # Slots missing on the earlier day only because of a DST shift: copy the neighbouring slot of the same day.
    day = pd.Series(local.normalize(), copy=False)
    frame = pd.DataFrame({"f": forecast, "day": day.to_numpy()})
    filled = frame.groupby("day")["f"].transform(lambda s: s.ffill().bfill()).to_numpy()
    dst_gap = np.isnan(forecast) & np.isfinite(filled) & (lag == 0)
    forecast[dst_gap] = filled[dst_gap]
    lag[dst_gap] = 1
    return forecast, lag


@dataclass
class ZoneMarket:
    """Realised and persistence-forecast prices of one zone on UTC quarter-hour and hour grids."""

    zone: str
    qh_utc: pd.DatetimeIndex
    da_price: np.ndarray
    da_resolution: np.ndarray  # minutes of the day-ahead product covering each QH
    hour_utc: pd.DatetimeIndex
    reserve: dict[str, np.ndarray] = field(default_factory=dict)
    da_forecast: np.ndarray = field(default_factory=lambda: np.zeros(0))
    da_forecast_lag: np.ndarray = field(default_factory=lambda: np.zeros(0, dtype=np.int64))
    reserve_forecast: dict[str, np.ndarray] = field(default_factory=dict)
    tz: str = MARKET_TZ

    @classmethod
    def from_frames(
        cls,
        zone: str,
        start_utc: pd.Timestamp,
        end_utc: pd.Timestamp,
        day_ahead: pd.DataFrame,
        reserves: Mapping[str, pd.DataFrame],
        *,
        tz: str = MARKET_TZ,
    ) -> ZoneMarket:
        """Build from a quarter-hourly day-ahead frame (``timestamp_utc``, ``resolution_minutes``,
        ``price_eur_mwh``) and hourly reserve frames (``delivery_start_utc``, ``price_eur_per_mw_h``) per key."""

        qh = pd.date_range(start_utc, end_utc, freq="15min", inclusive="left", tz="UTC")
        hours = pd.date_range(start_utc, end_utc, freq="1h", inclusive="left", tz="UTC")
        da = day_ahead.drop_duplicates("timestamp_utc").set_index("timestamp_utc")
        da_price = da["price_eur_mwh"].reindex(qh).to_numpy(np.float64, copy=True)
        da_res = da["resolution_minutes"].reindex(qh).fillna(15).to_numpy(np.int64, copy=True)
        reserve: dict[str, np.ndarray] = {}
        for key, frame in reserves.items():
            series = frame.drop_duplicates("delivery_start_utc").set_index("delivery_start_utc")["price_eur_per_mw_h"]
            values = series.reindex(hours).to_numpy(np.float64, copy=True)
            if (zone, key) in MISSING_MEANS_ZERO and len(series):
                inside = (hours >= series.index.min()) & np.isnan(values)
                values[inside] = 0.0
            if key == "fcr_dk1":
                values = _fill_within_blocks(values, hours, tz)
            reserve[key] = values
        market = cls(zone, qh, da_price, da_res, hours, reserve, tz=tz)
        market.da_forecast, market.da_forecast_lag = persistence_forecast(da_price, qh, tz)
        market.reserve_forecast = {k: persistence_forecast(v, hours, tz)[0] for k, v in reserve.items()}
        return market

    @classmethod
    def from_public(
        cls, zone: str, start_utc: pd.Timestamp, end_utc: pd.Timestamp, *, root: Path | None = None
    ) -> ZoneMarket:
        """Load ``zone`` from ``data/public/nordic`` with ``MAX_LOOKBACK_DAYS`` of history before ``start_utc``."""

        lo = start_utc - pd.Timedelta(days=MAX_LOOKBACK_DAYS + 1)
        da = load_nordic_day_ahead(zone, lo, end_utc, quarter_hourly=True, root=root)
        frames: dict[str, pd.DataFrame] = {}
        for product in zone_products(zone):
            direction = product.direction if product.table_product in {"afrr", "mfrr"} else None
            frames[product.key] = load_nordic_reserve(
                product.table_product, zone, lo, end_utc, auction="early", direction=direction, root=root
            )
        return cls.from_frames(zone, lo, end_utc, da, frames)

    def days(self, start_utc: pd.Timestamp, end_utc: pd.Timestamp) -> list[pd.Timestamp]:
        first = start_utc.tz_convert(self.tz).normalize()
        last = (end_utc - pd.Timedelta(seconds=1)).tz_convert(self.tz).normalize()
        days = pd.date_range(first.tz_localize(None), last.tz_localize(None), freq="D")
        return [pd.Timestamp(d) for d in days if local_day_bounds(pd.Timestamp(d), self.tz)[1] <= end_utc]

    def day_inputs(self, day: pd.Timestamp, *, forecast: bool) -> DayInputs | None:
        """Prices of ``day`` (realised or persistence forecast); ``None`` if the day-ahead prices are incomplete."""

        lo, hi = local_day_bounds(day, self.tz)
        q0, q1 = self.qh_utc.searchsorted(lo), self.qh_utc.searchsorted(hi)
        h0, h1 = self.hour_utc.searchsorted(lo), self.hour_utc.searchsorted(hi)
        if q1 - q0 != int((hi - lo) / pd.Timedelta(minutes=15)) or q0 == q1:
            return None
        known = np.isfinite(self.da_price[q0:q1])
        if int((~known).sum()) > MAX_MISSING_DA_QH:
            return None
        da = (self.da_forecast if forecast else self.da_price)[q0:q1].copy()
        if not np.isfinite(da[known]).all():
            return None
        da[~known] = 0.0
        source = self.reserve_forecast if forecast else self.reserve
        reserve = {k: v[h0:h1] for k, v in source.items()}
        local_hours = self.hour_utc[h0:h1].tz_convert(self.tz).hour.to_numpy()
        blocks = np.unique(local_hours // 4, return_inverse=True)[1].astype(np.int64)
        return DayInputs(
            da_price=da,
            qh_hour=np.arange(q1 - q0, dtype=np.int64) // 4,
            reserve_price=reserve,
            hour_block={4: blocks},
            hourly_da=bool((self.da_resolution[q0:q1] == 60).all()),
            da_tradable=None if known.all() else known,
        )

    def forecast_lag(self, day: pd.Timestamp) -> int:
        lo, hi = local_day_bounds(day, self.tz)
        q0, q1 = self.qh_utc.searchsorted(lo), self.qh_utc.searchsorted(hi)
        lags = self.da_forecast_lag[q0:q1]
        return int(lags.max()) if len(lags) else 0


def _fill_within_blocks(values: np.ndarray, hours: pd.DatetimeIndex, tz: str) -> np.ndarray:
    """DK1 FCR is traded in 4-hour blocks; fill an hour missing upstream (25-hour days) from its block."""

    local = hours.tz_convert(tz)
    key = pd.Series(local.normalize().tz_localize(None).astype("int64") // 10**9 * 10 + local.hour // 4)
    frame = pd.DataFrame({"v": values, "k": key.to_numpy()})
    return frame.groupby("k")["v"].transform(lambda s: s.ffill().bfill()).to_numpy(np.float64)


# --- runner -----------------------------------------------------------------------------------------------------


@dataclass
class DayRecord:
    zone: str
    workflow: str
    mode: str
    day: pd.Timestamp
    n_qh: int
    hourly_da: bool
    da_revenue_eur: float
    reserve_revenue_eur: dict[str, float]
    degradation_eur: float
    discharged_mwh: float
    charged_mwh: float
    avg_reserve_mw: dict[str, float]
    simultaneous_qh: int
    forecast_lag_days: int
    untradable_qh: int = 0

    def as_row(self) -> dict[str, Any]:
        row: dict[str, Any] = {
            "zone": self.zone,
            "workflow": self.workflow,
            "mode": self.mode,
            "day": self.day.date().isoformat(),
            "n_qh": self.n_qh,
            "hourly_da": self.hourly_da,
            "da_revenue_eur": self.da_revenue_eur,
        }
        for key in PRODUCT_ORDER:
            row[f"rev_{key}_eur"] = float(self.reserve_revenue_eur.get(key, 0.0))
        row["reserve_revenue_eur"] = float(sum(self.reserve_revenue_eur.values()))
        row["degradation_eur"] = self.degradation_eur
        row["net_eur"] = self.da_revenue_eur + row["reserve_revenue_eur"] - self.degradation_eur
        row["discharged_mwh"] = self.discharged_mwh
        row["charged_mwh"] = self.charged_mwh
        for key in PRODUCT_ORDER:
            row[f"avg_{key}_mw"] = float(self.avg_reserve_mw.get(key, 0.0))
        row["simultaneous_qh"] = self.simultaneous_qh
        row["forecast_lag_days"] = self.forecast_lag_days
        row["untradable_qh"] = self.untradable_qh
        return row


def _record(
    market: ZoneMarket,
    workflow: str,
    mode: str,
    day: pd.Timestamp,
    schedule: DaySchedule,
    realised: DayInputs,
    battery: BatteryParams,
) -> DayRecord:
    settled = settle_day(schedule, realised, battery)
    both = int(np.sum((schedule.charge_mw > 1e-6) & (schedule.discharge_mw > 1e-6)))
    return DayRecord(
        zone=market.zone,
        workflow=workflow,
        mode=mode,
        day=day,
        n_qh=realised.n_qh,
        hourly_da=realised.hourly_da,
        da_revenue_eur=settled.da_revenue_eur,
        reserve_revenue_eur=settled.reserve_revenue_eur,
        degradation_eur=settled.degradation_eur,
        discharged_mwh=settled.discharged_mwh,
        charged_mwh=settled.charged_mwh,
        avg_reserve_mw={k: float(np.mean(v)) for k, v in schedule.reserve_mw.items()},
        simultaneous_qh=both,
        forecast_lag_days=market.forecast_lag(day) if mode == "persistence" else 0,
        untradable_qh=0 if realised.da_tradable is None else int((~realised.da_tradable).sum()),
    )


def qh_ledger(
    market: ZoneMarket, day: pd.Timestamp, schedule: DaySchedule, realised: DayInputs, workflow: str, mode: str
) -> pd.DataFrame:
    """Quarter-hour ledger of one day in the valuation contract (``revenue_eur`` = day-ahead + capacity revenue,
    ``pnl_eur = revenue_eur - degradation_eur``) plus the reserved MW per product."""

    lo, _ = local_day_bounds(day, market.tz)
    q0 = market.qh_utc.searchsorted(lo)
    stamps = market.qh_utc[q0 : q0 + realised.n_qh]
    hours = realised.qh_hour
    net = schedule.discharge_mw - schedule.charge_mw
    tradable = np.ones(realised.n_qh, dtype=bool) if realised.da_tradable is None else np.asarray(realised.da_tradable)
    da_revenue = realised.da_price * net * QH_HOURS
    reserve_revenue = np.zeros(realised.n_qh)
    reserved: dict[str, np.ndarray] = {}
    for key, mw in schedule.reserve_mw.items():
        reserved[f"{key}_mw"] = mw[hours]
        price = np.nan_to_num(np.asarray(realised.reserve_price.get(key, np.zeros(len(mw)))))[hours]
        reserve_revenue += mw[hours] * price * QH_HOURS
    degradation = schedule.discharge_mw * QH_HOURS * NORDIC_BATTERY.degradation_eur_per_mwh
    frame = pd.DataFrame(
        {
            "timestamp_utc": pd.DatetimeIndex(stamps).astype("datetime64[ns, UTC]"),
            "energy_mwh": net * QH_HOURS,
            "discharged_mwh": schedule.discharge_mw * QH_HOURS,
            "charged_mwh": schedule.charge_mw * QH_HOURS,
            "base_price_eur_mwh": realised.da_price,
            "price_eur_mwh": realised.da_price,
            "price_missing": ~tradable,
            "da_revenue_eur": da_revenue,
            "reserve_revenue_eur": reserve_revenue,
            "revenue_eur": da_revenue + reserve_revenue,
            "degradation_eur": degradation,
            "pnl_eur": da_revenue + reserve_revenue - degradation,
            "soc_end_mwh": schedule.soc_mwh,
            **reserved,
        }
    )
    frame["strategy"] = f"nordic_{workflow}_{mode}"
    frame["mechanism"] = f"{market.zone} day-ahead + pay-as-cleared capacity"
    frame["fleet_mw"] = 0.0  # price taker on published prices
    frame["zone"] = market.zone
    return frame


@dataclass
class RunResult:
    daily: pd.DataFrame
    ledger: pd.DataFrame | None
    skipped_days: list[str]


def run_zone(
    market: ZoneMarket,
    start_utc: pd.Timestamp,
    end_utc: pd.Timestamp,
    *,
    workflows: Sequence[str] | None = None,
    modes: Sequence[str] = MODES,
    battery: BatteryParams = NORDIC_BATTERY,
    ledger_for: tuple[str, str] | None = ("full_stack", "persistence"),
    product_sets: Mapping[str, Sequence[ReserveProduct]] | None = None,
) -> RunResult:
    """Run every workflow and mode over the local days in ``[start_utc, end_utc)`` for one zone.

    ``product_sets`` (name -> products) replaces the standard workflows, e.g. for rule sensitivities.
    """

    sets: dict[str, list[ReserveProduct]]
    if product_sets is not None:
        sets = {name: list(products) for name, products in product_sets.items()}
    else:
        sets = {name: workflow_products(market.zone, name) for name in (workflows or applicable_workflows(market.zone))}
    rows: list[dict[str, Any]] = []
    ledgers: list[pd.DataFrame] = []
    skipped: list[str] = []
    for day in market.days(start_utc, end_utc):
        realised = market.day_inputs(day, forecast=False)
        forecast = market.day_inputs(day, forecast=True) if "persistence" in modes else None
        if realised is None or ("persistence" in modes and forecast is None):
            skipped.append(day.date().isoformat())
            continue
        for workflow, products in sets.items():
            for mode in modes:
                inputs = forecast if mode == "persistence" else realised
                assert inputs is not None
                schedule = solve_day(inputs, products, battery)
                rows.append(_record(market, workflow, mode, day, schedule, realised, battery).as_row())
                if ledger_for == (workflow, mode):
                    ledgers.append(qh_ledger(market, day, schedule, realised, workflow, mode))
    daily = pd.DataFrame(rows)
    ledger = pd.concat(ledgers, ignore_index=True) if ledgers else None
    return RunResult(daily=daily, ledger=ledger, skipped_days=skipped)


# --- summaries ----------------------------------------------------------------------------------------------------


REVENUE_COLUMNS = ["da_revenue_eur", *[f"rev_{k}_eur" for k in PRODUCT_ORDER], "degradation_eur", "net_eur"]


def annualise(daily: pd.DataFrame, energy_mwh: float = NORDIC_BATTERY.energy_mwh) -> dict[str, float]:
    """EUR/MW/yr per revenue column (sum x 365 / days), cycles/day and average reserved MW for a daily slice."""

    days = len(daily)
    if days == 0:
        return {"days": 0}
    out: dict[str, float] = {"days": float(days)}
    for column in REVENUE_COLUMNS:
        sign = -1.0 if column == "degradation_eur" else 1.0
        out[column] = sign * float(daily[column].sum()) * 365.0 / days
    out["cycles_per_day"] = float(daily["discharged_mwh"].sum()) / energy_mwh / days
    for key in PRODUCT_ORDER:
        column = f"avg_{key}_mw"
        if column in daily:
            out[column] = float(daily[column].mean())
    return out


def summarise(daily: pd.DataFrame, periods: Mapping[str, tuple[str, str]]) -> pd.DataFrame:
    """Annualised revenue per zone, workflow, mode and period (``periods``: label -> (first day, last day))."""

    rows = []
    for label, (first, last) in periods.items():
        mask = (daily["day"] >= first) & (daily["day"] <= last)
        for (zone, workflow, mode), group in daily.loc[mask].groupby(["zone", "workflow", "mode"], sort=False):
            rows.append({"period": label, "zone": zone, "workflow": workflow, "mode": mode, **annualise(group)})
    return pd.DataFrame(rows)


def capture_table(summary: pd.DataFrame) -> pd.DataFrame:
    """Persistence vs oracle per period, zone and workflow (``capture`` = persistence net / oracle net)."""

    keys = ["period", "zone", "workflow"]
    pers = summary.loc[summary["mode"] == "persistence"].set_index(keys)
    orac = summary.loc[summary["mode"] == "oracle"].set_index(keys)
    out = pers.join(orac[["net_eur"]].rename(columns={"net_eur": "oracle_net_eur"}), how="left")
    out["capture"] = out["net_eur"] / out["oracle_net_eur"]
    return out.reset_index()


def iter_periods(days: Iterable[str]) -> dict[str, tuple[str, str]]:
    """Calendar-year periods covered by ``days`` (ISO dates)."""

    ordered = sorted(days)
    if not ordered:
        return {}
    out: dict[str, tuple[str, str]] = {}
    for year in range(int(ordered[0][:4]), int(ordered[-1][:4]) + 1):
        inside = [d for d in ordered if d.startswith(str(year))]
        if inside:
            out[str(year)] = (inside[0], inside[-1])
    return out
