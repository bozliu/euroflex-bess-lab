"""Quarter-hour settlement of minute-level battery deviations, with an optional price-impact hook."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Protocol, runtime_checkable

import numpy as np
import pandas as pd
from numpy.typing import NDArray

from .types import MINUTE_HOURS, MINUTES_PER_QH, BatteryParams

FloatArray = NDArray[np.float64]


@runtime_checkable
class ImpactModel(Protocol):
    """Adjusts settlement prices for the fleet's own effect on the system imbalance.

    ``fleet_delta_mw`` is the fleet's average net injection over each quarter-hour (MW, + = more long). A long
    deviation raises the system imbalance and pushes the price down; implementations decide by how much.
    """

    @property
    def name(self) -> str: ...

    def adjust(self, qh_index: NDArray[np.int64], base_price: FloatArray, fleet_delta_mw: FloatArray) -> FloatArray: ...


@dataclass(frozen=True)
class NoImpact:
    name: str = "none"

    def adjust(self, qh_index: NDArray[np.int64], base_price: FloatArray, fleet_delta_mw: FloatArray) -> FloatArray:
        return np.asarray(base_price, dtype=float)


@dataclass(frozen=True)
class LinearImpact:
    """Price moves by ``-slope * fleet_delta_mw`` (EUR/MWh per MW). A transparent stress-test baseline."""

    slope_eur_mwh_per_mw: float
    name: str = "linear"

    def adjust(self, qh_index: NDArray[np.int64], base_price: FloatArray, fleet_delta_mw: FloatArray) -> FloatArray:
        return np.asarray(base_price, dtype=float) - self.slope_eur_mwh_per_mw * np.asarray(fleet_delta_mw)


def settle_quarter_hours(
    power_mw: FloatArray,
    soc_mwh: FloatArray,
    base_price: FloatArray,
    qh_times: pd.DatetimeIndex,
    battery: BatteryParams,
    *,
    impact: ImpactModel | None = None,
    fleet_scale: float = 1.0,
) -> pd.DataFrame:
    """Aggregate a minute power trace into the quarter-hour P&L ledger.

    The BRP's imbalance energy per quarter-hour is the integral of the deviation; a long position is paid the
    single imbalance price, a short one pays it. Quarter-hours with a missing price earn nothing and are flagged.
    """
    n_qh = len(qh_times)
    if len(power_mw) != n_qh * MINUTES_PER_QH:
        raise ValueError("power trace length must be 15 x number of quarter-hours")
    by_qh = power_mw.reshape(n_qh, MINUTES_PER_QH)
    energy = by_qh.sum(axis=1) * MINUTE_HOURS
    discharged = np.clip(by_qh, 0.0, None).sum(axis=1) * MINUTE_HOURS
    charged = np.clip(-by_qh, 0.0, None).sum(axis=1) * MINUTE_HOURS
    base = np.asarray(base_price, dtype=float)
    price = base
    if impact is not None:
        fleet_delta_mw = energy / 0.25 * fleet_scale
        price = impact.adjust(np.arange(n_qh, dtype=np.int64), base, fleet_delta_mw)
    missing = np.isnan(price)
    revenue = np.where(missing, 0.0, energy * np.nan_to_num(price))
    degradation = discharged * battery.degradation_eur_per_mwh
    soc_end = soc_mwh.reshape(n_qh, MINUTES_PER_QH)[:, -1]
    return pd.DataFrame(
        {
            "timestamp_utc": qh_times,
            "energy_mwh": energy,
            "discharged_mwh": discharged,
            "charged_mwh": charged,
            "base_price_eur_mwh": base,
            "price_eur_mwh": price,
            "price_missing": missing,
            "revenue_eur": revenue,
            "degradation_eur": degradation,
            "pnl_eur": revenue - degradation,
            "soc_end_mwh": soc_end,
        }
    )
