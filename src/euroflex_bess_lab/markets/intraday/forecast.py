"""Causal forecasts for the two-stage strategy.

* Day-ahead stage: persistence by local clock time (previous local day; first occurrence for clock times that a
  25-hour day repeats, previous clock for times a 23-hour day lacks), the semantics of the repository's
  ``PersistenceForecastProvider``.
* Intraday stage: ``ID_hat_j = DA_j + profile[slot_j] + beta[lead_j] * r_bar``

  - ``profile``: mean intraday-minus-day-ahead spread per local quarter-hour of the day over the
    ``profile_days`` local days before D-1 (days D-1-W .. D-2, i.e. already published when day D is traded);
  - ``r_bar``: mean residual (spread minus profile) of the ``residual_window`` most recent products whose
    closing price is visible at the decision time (trade-feed model);
  - ``beta[lead]``: direct least-squares slope of the residual ``lead`` quarter-hours ahead on ``r_bar``,
    estimated on the same trailing window and clipped to ``[0, 1]``.

  Spreads are winsorised at ``+-winsor_eur_mwh`` for estimation and for ``r_bar`` (execution uses raw prices).
"""

from __future__ import annotations

from dataclasses import dataclass
from datetime import date, timedelta

import numpy as np
from numpy.typing import NDArray

from .config import ForecastSettings
from .data import IntradayMarket

FloatArray = NDArray[np.float64]
N_SLOTS = 96


def _local_clock_minutes(market: IntradayMarket, lo: int, hi: int) -> NDArray[np.int64]:
    return (market.slot[lo:hi] * 15).astype(np.int64)


def persistence_day_ahead(market: IntradayMarket, day: date) -> tuple[FloatArray, date]:
    """Day-ahead price forecast for ``day`` known at D-1 12:00: the previous available local day by clock time.

    Returns ``(forecast, source_day)``; ``source_day`` is D-1 unless that day is missing from the market.
    """

    lo, hi = market.day_bounds(day)
    source = day - timedelta(days=1)
    while not market.has_day(source):
        source -= timedelta(days=1)
        if source < market.days[0]:
            raise ValueError(f"no earlier day-ahead day available for persistence of {day}")
    slo, shi = market.day_bounds(source)
    src_clock = _local_clock_minutes(market, slo, shi)
    src_price = market.da_price[slo:shi]
    # first occurrence of every clock time (a 25-hour day repeats 02:00-02:45)
    _, first = np.unique(src_clock, return_index=True)
    first = np.sort(first)
    clocks, prices = src_clock[first], src_price[first]
    order = np.argsort(clocks, kind="stable")
    clocks, prices = clocks[order], prices[order]
    target = _local_clock_minutes(market, lo, hi)
    pos = np.searchsorted(clocks, target, side="right") - 1
    if (pos < 0).any():
        raise ValueError(f"persistence source {source} does not cover the first clock times of {day}")
    return prices[pos].astype(float), source


def clipped_spread(market: IntradayMarket, lo: int, hi: int, winsor: float) -> FloatArray:
    spread = market.id_price[lo:hi] - market.da_price[lo:hi]
    out: FloatArray = np.clip(spread, -winsor, winsor)
    return out


def _rolling_mean(values: FloatArray, window: int) -> FloatArray:
    """Trailing nan-mean over ``window`` values ending at each position (NaN if none valid)."""

    valid = np.isfinite(values)
    filled = np.where(valid, values, 0.0)
    csum = np.concatenate([[0.0], np.cumsum(filled)])
    ccount = np.concatenate([[0], np.cumsum(valid.astype(np.int64))])
    idx = np.arange(len(values))
    start = np.maximum(idx - window + 1, 0)
    total = csum[idx + 1] - csum[start]
    count = ccount[idx + 1] - ccount[start]
    with np.errstate(invalid="ignore", divide="ignore"):
        mean = np.where(count > 0, total / np.maximum(count, 1), np.nan)
    return mean


@dataclass(frozen=True)
class DaySpreadModel:
    """Fitted intraday spread model for one delivery day (all inputs visible before day D is traded)."""

    day: date
    profile: FloatArray  # (96,) mean spread per local quarter-hour slot
    beta: FloatArray  # (max_lead + 1,), beta[0] unused
    history_days: int
    settings: ForecastSettings

    def base(self, market: IntradayMarket, lo: int, hi: int) -> FloatArray:
        """``DA_j + profile[slot_j]`` for the products ``[lo, hi)``."""

        out: FloatArray = market.da_price[lo:hi] + self.profile[market.slot[lo:hi]]
        return out

    def residuals(self, market: IntradayMarket, lo: int, hi: int) -> FloatArray:
        spread = clipped_spread(market, lo, hi, self.settings.winsor_eur_mwh)
        out: FloatArray = spread - self.profile[market.slot[lo:hi]]
        return out

    def lead_weights(self, offset: int, count: int) -> FloatArray:
        """``beta[offset + i]`` for ``i in range(count)`` (zero beyond ``max_lead``)."""

        leads = offset + np.arange(count)
        weights = np.zeros(count)
        inside = leads < len(self.beta)
        weights[inside] = self.beta[leads[inside]]
        return weights


def fit_spread_model(market: IntradayMarket, day: date, settings: ForecastSettings) -> DaySpreadModel:
    """Fit profile and residual persistence on local days ``[D-1-W, D-1)`` (strictly before D-1)."""

    window_days = [
        day - timedelta(days=offset)
        for offset in range(settings.profile_days + 1, 1, -1)
        if market.has_day(day - timedelta(days=offset))
    ]
    profile = np.zeros(N_SLOTS)
    beta = np.zeros(settings.max_lead + 1)
    if not window_days:
        return DaySpreadModel(day, profile, beta, 0, settings)
    segments = [market.day_bounds(d) for d in window_days]
    spreads = [clipped_spread(market, lo, hi, settings.winsor_eur_mwh) for lo, hi in segments]
    slots = [market.slot[lo:hi] for lo, hi in segments]
    spread_all = np.concatenate(spreads)
    slot_all = np.concatenate(slots)
    profile = slot_profile(
        spread_all, slot_all, shrinkage=settings.profile_shrinkage, threshold_se=settings.profile_threshold_se
    )
    # Residual persistence on the contiguous window (days are consecutive except for skipped market days).
    lo0 = segments[0][0]
    hi0 = segments[-1][1]
    contiguous = all(segments[i][1] == segments[i + 1][0] for i in range(len(segments) - 1))
    if contiguous:
        resid = clipped_spread(market, lo0, hi0, settings.winsor_eur_mwh) - profile[market.slot[lo0:hi0]]
        beta = residual_betas(resid, settings.residual_window, settings.max_lead, settings.beta_min_t)
    return DaySpreadModel(day, profile, beta, len(window_days), settings)


def slot_profile(
    spread: FloatArray, slot: NDArray[np.int64], *, shrinkage: str = "soft", threshold_se: float = 2.0
) -> FloatArray:
    """Mean spread per local quarter-hour slot, shrunk towards zero per slot.

    ``"soft"``: ``sign(mean) * max(|mean| - threshold_se * se, 0)`` (soft thresholding at ``threshold_se`` standard
    errors); ``"js"``: positive-part James-Stein factor ``max(0, 1 - se^2 / mean^2)``; ``"none"``: raw means.
    """

    valid = np.isfinite(spread)
    s, k = spread[valid], slot[valid]
    n = np.bincount(k, minlength=N_SLOTS).astype(float)
    s1 = np.bincount(k, weights=s, minlength=N_SLOTS)
    mean = np.where(n > 0, s1 / np.maximum(n, 1.0), 0.0)
    if shrinkage == "none":
        out: FloatArray = mean
        return out
    s2 = np.bincount(k, weights=s * s, minlength=N_SLOTS)
    var = np.where(n > 1, (s2 - n * mean**2) / np.maximum(n - 1.0, 1.0), 0.0)
    se = np.sqrt(np.maximum(var, 0.0) / np.maximum(n, 1.0))
    if shrinkage == "js":
        factor = np.where(n > 1, np.clip(1.0 - se**2 / np.maximum(mean**2, 1e-12), 0.0, 1.0), 0.0)
        shrunk: FloatArray = mean * factor
        return shrunk
    if shrinkage != "soft":
        raise ValueError(f"unknown profile shrinkage {shrinkage!r}; expected 'none', 'js' or 'soft'")
    soft: FloatArray = np.where(n > 1, np.sign(mean) * np.maximum(np.abs(mean) - threshold_se * se, 0.0), 0.0)
    return soft


def residual_betas(resid: FloatArray, window: int, max_lead: int, min_t: float) -> FloatArray:
    """Direct slopes ``beta[L]`` of ``resid[t + L]`` on the trailing mean ``r_bar[t]`` (clipped to [0, 1]);
    slopes with a t-statistic below ``min_t`` are set to zero."""

    beta = np.zeros(max_lead + 1)
    rbar = _rolling_mean(resid, window)
    for lead in range(1, max_lead + 1):
        x = rbar[:-lead]
        y = resid[lead:]
        ok = np.isfinite(x) & np.isfinite(y)
        m = int(ok.sum())
        if m < 96:
            continue
        xo, yo = x[ok], y[ok]
        denom = float(np.dot(xo, xo))
        if denom <= 0.0:
            continue
        slope = float(np.dot(xo, yo) / denom)
        se = float(np.sqrt(np.sum((yo - slope * xo) ** 2) / max(m - 1, 1) / denom))
        if se > 0.0 and slope / se < min_t:
            continue
        beta[lead] = float(np.clip(slope, 0.0, 1.0))
    return beta


@dataclass
class LiveForecaster:
    """Per-day intraday price forecaster for the rolling strategy (trade-feed information model)."""

    model: DaySpreadModel
    market: IntradayMarket
    lo: int
    hi: int
    visible_offset: int

    def __post_init__(self) -> None:
        settings = self.model.settings
        self._base = self.model.base(self.market, self.lo, self.hi)
        start = max(0, self.lo - self.visible_offset - settings.residual_window + 1)
        self._resid_start = start
        self._resid = self.model.residuals(self.market, start, self.hi)

    def visible_end(self, k: int) -> int:
        """Exclusive global index of the products whose closing price is usable for the decision on product k."""

        return self.lo + k - self.visible_offset + 1

    def residual_mean(self, k: int) -> float:
        end = self.visible_end(k)  # exclusive
        start = max(end - self.model.settings.residual_window, self._resid_start)
        if end <= start:
            return 0.0
        window = self._resid[start - self._resid_start : end - self._resid_start]
        window = window[np.isfinite(window)]
        return float(window.mean()) if window.size else 0.0

    def forecast(self, k: int) -> FloatArray:
        """Forecast of the execution price for products ``k .. n-1`` of the day at the decision time of ``k``."""

        n = self.hi - self.lo
        r_bar = self.residual_mean(k)
        weights = self.model.lead_weights(self.visible_offset + 0, n - k)
        out: FloatArray = self._base[k:] + weights * r_bar
        return out
