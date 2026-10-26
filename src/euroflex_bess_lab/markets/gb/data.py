"""Settlement-period market bundle for the GB backtest (numpy arrays on a regular 30-minute UTC grid)."""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import pandas as pd
from numpy.typing import NDArray

from ...public_data.gb.ecb import gbp_per_eur_on
from ...public_data.gb.loaders import (
    load_gb_fx,
    load_gb_market_index,
    load_gb_response_auctions,
    load_gb_response_energy,
    load_gb_system_prices,
)
from ...public_data.gb.timeutils import UK_TZ, efa_date_block, settlement_date_period, sp_grid, to_utc
from .rules import RESPONSE_PRODUCTS

FloatArray = NDArray[np.float64]
IntArray = NDArray[np.int64]


@dataclass
class GBMarket:
    """Aligned arrays for ``n`` settlement periods starting at ``ts[0]``.

    ``eac_price[product][k]`` is the clearing price (GBP/MW/h) of the EFA block containing SP ``k``;
    ``response_energy[product][k]`` the MWh delivered per MW of that product in SP ``k`` (from frequency).
    """

    ts: pd.DatetimeIndex
    mid: FloatArray
    sip: FloatArray
    niv: FloatArray
    gbp_per_eur: FloatArray
    response_energy: dict[str, FloatArray]
    eac_price: dict[str, FloatArray]
    efa_day: IntArray  # index into ``efa_dates``
    efa_block: IntArray  # 1..6
    efa_dates: list[str]
    settlement_date: NDArray[np.object_]
    settlement_period: IntArray
    local_hour: IntArray
    local_minute: IntArray
    energy_filled_sps: int = 0
    meta: dict[str, object] = field(default_factory=dict)

    @property
    def n(self) -> int:
        return len(self.ts)

    @property
    def mid_settle(self) -> FloatArray:
        """MID with the (rare) SPs without traded MID volume settled at the system price instead."""

        return np.where(np.isfinite(self.mid), self.mid, self.sip)

    def block_sps(self, day: int, block: int) -> IntArray:
        return np.flatnonzero((self.efa_day == day) & (self.efa_block == block)).astype(np.int64)

    def day_sps(self, day: int) -> IntArray:
        return np.flatnonzero(self.efa_day == day).astype(np.int64)

    def index_of(self, ts: object) -> int:
        stamp = to_utc(ts)
        pos = int(self.ts.searchsorted(stamp))
        if pos >= self.n or self.ts[pos] != stamp:
            raise KeyError(f"{stamp} is not a settlement-period start of this market bundle")
        return pos

    def eac_gate_sps(self) -> IntArray:
        """Indices of SPs starting at 14:00 UK (EAC gate closure for the next EFA day)."""

        return np.flatnonzero((self.local_hour == 14) & (self.local_minute == 0)).astype(np.int64)

    @classmethod
    def from_frames(
        cls,
        system_prices: pd.DataFrame,
        market_index: pd.DataFrame,
        response_auctions: pd.DataFrame,
        response_energy: pd.DataFrame,
        fx: pd.DataFrame,
        *,
        start: object,
        end: object,
    ) -> GBMarket:
        index = sp_grid(start, end)
        ts = pd.DatetimeIndex(index).as_unit("ns")

        def aligned(frame: pd.DataFrame, column: str, time_column: str = "timestamp_utc") -> FloatArray:
            series = frame.set_index(pd.DatetimeIndex(pd.to_datetime(frame[time_column], utc=True)).as_unit("ns"))[
                column
            ]
            series = series[~series.index.duplicated(keep="last")]
            return series.reindex(ts).to_numpy(dtype=float)

        sip = aligned(system_prices, "system_price_gbp_mwh")
        niv = aligned(system_prices, "niv_mwh")
        mid = aligned(market_index, "mid_price_gbp_mwh")
        energy: dict[str, FloatArray] = {}
        filled = 0
        for product in RESPONSE_PRODUCTS:
            values = aligned(response_energy, f"{product.lower()}_mwh_per_mw")
            missing = ~np.isfinite(values)
            filled = max(filled, int(missing.sum()))
            # SPs without enough frequency samples: trailing 7-day mean of the product (causal fill)
            trailing = pd.Series(values).rolling(7 * 48, min_periods=1).mean().shift(1).to_numpy()
            fallback = float(np.nanmean(values)) if np.isfinite(values).any() else 0.0
            energy[product] = np.where(missing, np.nan_to_num(trailing, nan=fallback), values)
        efa_dates_sp, blocks = efa_date_block(ts)
        efa_dates = sorted(set(efa_dates_sp.tolist()))
        day_lookup = {d: i for i, d in enumerate(efa_dates)}
        efa_day = np.array([day_lookup[d] for d in efa_dates_sp], dtype=np.int64)
        eac: dict[str, FloatArray] = {}
        auctions = response_auctions.copy()
        auctions["efa_date"] = auctions["efa_date"].astype(str)
        for product in RESPONSE_PRODUCTS:
            part = auctions[auctions["product"] == product]
            lookup = {
                (str(d), int(b)): float(p)
                for d, b, p in zip(
                    part["efa_date"], part["efa_block"], part["clearing_price_gbp_per_mw_h"], strict=True
                )
            }
            eac[product] = np.array(
                [lookup.get((d, int(b)), np.nan) for d, b in zip(efa_dates_sp, blocks, strict=True)], dtype=float
            )
        fx_rates = gbp_per_eur_on(ts, fx)
        dates, periods = settlement_date_period(ts)
        local = ts.tz_convert(UK_TZ)
        return cls(
            ts=ts,
            mid=mid,
            sip=sip,
            niv=niv,
            gbp_per_eur=np.asarray(fx_rates, dtype=float),
            response_energy=energy,
            eac_price=eac,
            efa_day=efa_day,
            efa_block=np.asarray(blocks, dtype=np.int64),
            efa_dates=efa_dates,
            settlement_date=dates,
            settlement_period=np.asarray(periods, dtype=np.int64),
            local_hour=np.asarray(local.hour, dtype=np.int64),
            local_minute=np.asarray(local.minute, dtype=np.int64),
            energy_filled_sps=filled,
            meta={"start_utc": str(ts[0]) if len(ts) else None, "end_utc": str(to_utc(end))},
        )

    @classmethod
    def from_public(cls, start: object, end: object, *, root: Path | None = None) -> GBMarket:
        """Load from ``data/public/gb`` (EAC history from 60 days before ``start`` is not needed here)."""

        lo, hi = to_utc(start), to_utc(end)
        sp = load_gb_system_prices(lo, hi, ["system_price_gbp_mwh", "niv_mwh"], root=root)
        mid = load_gb_market_index(lo, hi, ["mid_price_gbp_mwh"], root=root)
        auctions = load_gb_response_auctions(lo - pd.Timedelta(days=2), hi + pd.Timedelta(days=1), root=root)
        energy = load_gb_response_energy(lo, hi, root=root)
        fx = load_gb_fx(lo - pd.Timedelta(days=14), hi, root=root)
        return cls.from_frames(sp, mid, auctions, energy, fx, start=lo, end=hi)
