"""Causal forecasts for the GB strategies (every input is published before the decision time).

All settings below were fixed before looking at results from 2025 or 2026 (the level-correction decay was chosen
on December 2023 - June 2024 only; see ``docs/great_britain.md``).

* :class:`PriceForecaster` - MID and system-price forecasts for any target SP from a decision time:
  same-slot persistence from the latest *published* day, a decaying level correction from the latest published
  SP, and for the system price a spread (system price - MID) term built from the latest published spread with
  trailing 28-day slot means and autocorrelations.
* :func:`eac_persistence_forecast` - clearing price of the same EFA block on the latest published EFA day.
* :func:`expected_response_energy` - trailing 28-day mean of the delivered energy per MW for each product and EFA
  block (from frequency, which the operator observes in real time).
"""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd
from numpy.typing import NDArray

from .data import GBMarket
from .rules import MID_PUBLICATION_LAG_MINUTES, RESPONSE_PRODUCTS, SYSTEM_PRICE_PUBLICATION_LAG_MINUTES

FloatArray = NDArray[np.float64]
IntArray = NDArray[np.int64]
SLOTS = 48
SPREAD_CLIP_GBP = 200.0


def last_usable_index(t: int, publication_lag_minutes: int) -> int:
    """Index of the latest SP whose value is usable at the start of SP ``t`` (value of SP k usable from
    ``T_k + 30 min + lag``)."""

    return t - 1 - int(np.ceil(publication_lag_minutes / 30.0))


@dataclass(frozen=True)
class ForecastSettings:
    """``profile_days``/``latest_weight``: the same-slot base forecast is ``latest_weight`` x the latest published
    day + the rest x the mean of the latest ``profile_days`` published days; ``level_decay_per_sp`` scales the
    level correction from the latest published SP (0 disables it)."""

    level_decay_per_sp: float = 0.9
    profile_days: int = 1
    latest_weight: float = 1.0
    spread_window_days: int = 28
    spread_max_lag: int = 96
    use_spread: bool = True


class PriceForecaster:
    """Vectorised causal forecasts on the market grid."""

    def __init__(
        self,
        market: GBMarket,
        settings: ForecastSettings | None = None,
        *,
        mid_lag_minutes: int = MID_PUBLICATION_LAG_MINUTES,
        sip_lag_minutes: int = SYSTEM_PRICE_PUBLICATION_LAG_MINUTES,
    ) -> None:
        self.market = market
        self.settings = settings or ForecastSettings()
        self.mid_lag = mid_lag_minutes
        self.sip_lag = sip_lag_minutes
        # SPs without traded MID volume (rare): carry the previous SP's MID forward (causal); before the first
        # traded SP fall back to that SP's system price
        self.mid = pd.Series(market.mid).ffill().fillna(pd.Series(market.sip)).to_numpy(dtype=float)
        self.sip = market.sip.copy()
        self.spread = np.clip(self.sip - self.mid, -SPREAD_CLIP_GBP, SPREAD_CLIP_GBP)
        self._daily_stats: dict[int, tuple[FloatArray, FloatArray]] = {}

    # ---------------------------------------------------------------------------------------------- MID
    def mid_forecast(self, t: int, targets: IntArray) -> FloatArray:
        """Forecast MID of ``targets`` (SP indices >= the first unpublished SP) at the start of SP ``t``."""

        lm = last_usable_index(t, self.mid_lag)
        return self._persistence(self.mid, lm, targets)

    def _persistence(self, series: FloatArray, last: int, targets: IntArray) -> FloatArray:
        targets = np.asarray(targets, dtype=np.int64)
        if last < 0:
            return np.full(len(targets), np.nan)
        lead = targets - last
        days_back = np.maximum(np.ceil(lead / SLOTS).astype(np.int64), 1)
        source = targets - SLOTS * days_back
        source = np.clip(source, 0, last)
        out = series[source].astype(float)
        bad = ~np.isfinite(out)
        if bad.any():
            out[bad] = series[np.clip(source[bad] - SLOTS, 0, last)]
        settings = self.settings
        if settings.profile_days > 1 and settings.latest_weight < 1.0:
            stack = np.stack(
                [series[np.clip(source - SLOTS * i, 0, last)] for i in range(settings.profile_days)], axis=0
            )
            profile = np.nanmean(stack, axis=0)
            out = settings.latest_weight * out + (1.0 - settings.latest_weight) * np.where(
                np.isfinite(profile), profile, out
            )
        # level correction: latest published value against the same slot one day earlier, decaying with lead
        ref = last - SLOTS
        if ref >= 0 and np.isfinite(series[last]) and np.isfinite(series[ref]):
            out = out + (self.settings.level_decay_per_sp ** np.maximum(lead, 0)) * (series[last] - series[ref])
        return np.nan_to_num(out, nan=float(np.nanmean(series[max(0, last - 7 * SLOTS) : last + 1])))

    # ------------------------------------------------------------------------------------ system price
    def _spread_stats(self, day: int, last: int) -> tuple[FloatArray, FloatArray]:
        """Slot means and autocorrelations (lags 0..max_lag) of the clipped spread over the trailing window,
        computed once per UTC day from data published at that day's start."""

        cached = self._daily_stats.get(day)
        if cached is not None:
            return cached
        window = self.settings.spread_window_days * SLOTS
        lo = max(0, last - window + 1)
        x = self.spread[lo : last + 1]
        slots = (np.arange(lo, last + 1) % SLOTS).astype(np.int64)
        finite = np.isfinite(x)
        means = np.zeros(SLOTS)
        if finite.any():
            sums = np.bincount(slots[finite], weights=x[finite], minlength=SLOTS)
            counts = np.bincount(slots[finite], minlength=SLOTS)
            means = np.where(counts > 0, sums / np.maximum(counts, 1), 0.0)
        dev = np.where(finite, x - means[slots], 0.0)
        var = float(np.dot(dev, dev))
        max_lag = self.settings.spread_max_lag
        acf = np.zeros(max_lag + 1)
        if var > 0 and len(dev) > max_lag + 10:
            for lag in range(max_lag + 1):
                acf[lag] = float(np.dot(dev[lag:], dev[: len(dev) - lag])) / var
        acf = np.clip(acf, 0.0, 1.0)  # use only positive persistence
        self._daily_stats[day] = (means, acf)
        return means, acf

    def spread_forecast(self, t: int, targets: IntArray) -> FloatArray:
        targets = np.asarray(targets, dtype=np.int64)
        ls = last_usable_index(t, self.sip_lag)
        if ls < SLOTS or not self.settings.use_spread:
            return np.zeros(len(targets))
        day_start = (t // SLOTS) * SLOTS
        means, acf = self._spread_stats(t // SLOTS, last_usable_index(day_start, self.sip_lag))
        lag = np.clip(targets - ls, 0, len(acf) - 1)
        last_dev = self.spread[ls] - means[ls % SLOTS] if np.isfinite(self.spread[ls]) else 0.0
        return means[targets % SLOTS] + acf[lag] * last_dev

    def sip_forecast(self, t: int, targets: IntArray) -> FloatArray:
        """System price forecast = MID forecast + spread forecast."""

        return self.mid_forecast(t, targets) + self.spread_forecast(t, targets)

    def trailing_mean(self, series: str, t: int, days: int = 7) -> float:
        values = self.mid if series == "mid" else self.sip
        last = last_usable_index(t, self.mid_lag if series == "mid" else self.sip_lag)
        lo = max(0, last - days * SLOTS + 1)
        window = values[lo : last + 1]
        return float(np.nanmean(window)) if np.isfinite(window).any() else 0.0


def eac_persistence_forecast(market: GBMarket, target_day: int, fallback_days: int = 7) -> list[dict[str, float]]:
    """Per EFA block (1..6) and product: clearing price of the same block on EFA day ``target_day - 1``
    (published at 15:00 UK two days before the target day, i.e. before the 14:00 gate closure for the target)."""

    out: list[dict[str, float]] = []
    for block in range(1, 7):
        prices: dict[str, float] = {}
        for product in RESPONSE_PRODUCTS:
            value = float("nan")
            for back in range(1, fallback_days + 1):
                sps = np.flatnonzero((market.efa_day == target_day - back) & (market.efa_block == block))
                if len(sps):
                    candidate = float(market.eac_price[product][sps[0]])
                    if np.isfinite(candidate):
                        value = candidate
                        break
            prices[product] = value
        out.append(prices)
    return out


def realised_block_prices(market: GBMarket, day: int) -> list[dict[str, float]]:
    out: list[dict[str, float]] = []
    for block in range(1, 7):
        sps = market.block_sps(day, block)
        out.append({p: float(market.eac_price[p][sps[0]]) if len(sps) else float("nan") for p in RESPONSE_PRODUCTS})
    return out


def expected_response_energy(market: GBMarket, t: int, days: int = 28) -> dict[str, FloatArray]:
    """Mean delivered energy per MW and SP for each product by EFA block (index 0..5), from the ``days`` days of
    frequency data before the start of SP ``t``."""

    lo = max(0, t - days * SLOTS)
    blocks = market.efa_block[lo:t] - 1
    out: dict[str, FloatArray] = {}
    for product in RESPONSE_PRODUCTS:
        values = market.response_energy[product][lo:t]
        if len(values) == 0:
            out[product] = np.zeros(6)
            continue
        sums = np.bincount(blocks, weights=values, minlength=6)
        counts = np.bincount(blocks, minlength=6)
        overall = float(values.mean())
        out[product] = np.where(counts > 0, sums / np.maximum(counts, 1), overall)
    return out


def replacement_prices(
    market: GBMarket,
    t: int,
    *,
    days: int = 28,
    lags: tuple[int, ...] = (2, 3, 4),
    sip_lag_minutes: int = SYSTEM_PRICE_PUBLICATION_LAG_MINUTES,
) -> tuple[dict[str, float], float]:
    """Energy-weighted system price at which each product's delivered energy is replaced, and the plain mean.

    Low-frequency delivery (export) coincides with a short system, so the SOC is restored a few SPs later at an
    above-average system price; high-frequency delivery (import) is sold back below average. For each product the
    price is the mean system price ``lags`` SPs after each SP, weighted by the product's delivered energy, over the
    trailing ``days`` days whose system prices are published at the start of SP ``t``.
    """

    last = last_usable_index(t, sip_lag_minutes)
    hi = last - max(lags)
    lo = max(0, last - days * SLOTS)
    sip = market.sip
    if hi <= lo:
        mean = float(np.nanmean(sip[: max(last, 1)])) if last > 0 else 0.0
        return {p: mean for p in RESPONSE_PRODUCTS}, mean
    later = np.mean(np.stack([sip[lo + lag : hi + lag] for lag in lags], axis=0), axis=0)
    mean = float(np.nanmean(sip[lo : last + 1]))
    out: dict[str, float] = {}
    for product in RESPONSE_PRODUCTS:
        weights = market.response_energy[product][lo:hi]
        ok = np.isfinite(later) & np.isfinite(weights)
        total = float(weights[ok].sum())
        out[product] = float((later[ok] * weights[ok]).sum() / total) if total > 0 else mean
    return out, mean
