"""Precision-weighted fusion of a forecast with the live running quarter-hour-to-date price.

Elia publishes, every minute, the running imbalance price of the current quarter-hour under the formula in force.
Late in the quarter-hour that running value is the best predictor of the final price, but a forecaster trained
without history of a *new* formula cannot have learned to use it (zero-history transfer). The fusion treats the
live running value ``r`` as a noisy observation of the final price whose error variance depends only on how many
minutes of the quarter-hour are visible, and combines it with the forecaster's distribution (mean ``mu``,
variance ``v0``) like a Gaussian update:

    mean = (mu / v0 + r / v_r) / (1 / v0 + 1 / v_r),      spread scaled by sqrt(v_post / v0).

``v_r`` per visible-minute count is estimated on a training window with the training mechanism's own running field
(e.g. ODS133 current-formula running price vs ODS134 final price). Applying it to another mechanism's running
field assumes the convergence profile of the quarter-hour-to-date estimate is mechanism-invariant to first order
(checked in ``reports/realtime_belgium.md``: both formulas converge alike). No parameter is tuned.
"""

from __future__ import annotations

from dataclasses import replace

import numpy as np
import pandas as pd
from numpy.typing import NDArray

from ..replay import MarketData
from ..types import MINUTES_PER_QH, PublicationModel
from .base import DiscreteForecast, ForecastFrame, SupportsForecast
from .distribution import distribution_mean
from .features import _ffill_within_qh, _Index, _take, minute_target
from .splits import minute_range

FloatArray = NDArray[np.float64]


def running_estimate(market: MarketData, field: str, publication: PublicationModel) -> tuple[FloatArray, FloatArray]:
    """Visible running value of ``field`` in the current quarter-hour and the number of visible minutes."""
    ix = _Index.build(market.n_minutes, publication)
    values = market.minute.get(field)
    if values is None:
        return np.full(market.n_minutes, np.nan), ix.n_visible.astype(float)
    filled = _ffill_within_qh(np.asarray(values, dtype=float), 2)
    return _take(filled, ix.r, ix.in_qh), ix.n_visible.astype(float)


def running_error_variance(
    market: MarketData,
    field: str,
    final_prices_qh: FloatArray,
    publication: PublicationModel,
    start: pd.Timestamp,
    end: pd.Timestamp,
) -> FloatArray:
    """Mean squared error of the visible running value vs the final price, per number of visible minutes (0..15)."""
    running, n_visible = running_estimate(market, field, publication)
    final = minute_target(market, final_prices_qh, 0)
    m0, m1 = minute_range(market, start, end)
    rows = np.arange(m0, m1)
    err = (running[rows] - final[rows]) ** 2
    nv = n_visible[rows].astype(int)
    out = np.full(MINUTES_PER_QH + 1, np.nan)
    for k in range(1, MINUTES_PER_QH + 1):
        sel = (nv == k) & np.isfinite(err)
        if sel.sum() >= 50:
            out[k] = float(np.mean(err[sel]))
    return out


class RunningEstimateFusion:
    """Wraps a forecaster and fuses its current-quarter-hour distribution with a live running price field."""

    def __init__(self, base: SupportsForecast, running_field: str, error_variance: FloatArray, name: str | None = None):
        self.forecaster = base
        self.running_field = running_field
        self.error_variance = np.asarray(error_variance, dtype=float)
        self.name = name or f"{base.name}+live"

    @classmethod
    def fit(
        cls,
        base: SupportsForecast,
        market: MarketData,
        *,
        running_field: str,
        train_running_field: str,
        train_prices_qh: FloatArray,
        start: str | pd.Timestamp,
        end: str | pd.Timestamp,
        publication: PublicationModel | None = None,
        name: str | None = None,
    ) -> RunningEstimateFusion:
        """Estimate the running-estimate error profile on ``[start, end)`` with ``train_running_field``."""
        publication = publication or PublicationModel()
        s, e = pd.Timestamp(start), pd.Timestamp(end)
        s = s.tz_localize("UTC") if s.tzinfo is None else s.tz_convert("UTC")
        e = e.tz_localize("UTC") if e.tzinfo is None else e.tz_convert("UTC")
        variance = running_error_variance(market, train_running_field, train_prices_qh, publication, s, e)
        return cls(base, running_field, variance, name)

    def predict(
        self,
        market: MarketData,
        publication: PublicationModel | None = None,
        rows: NDArray[np.int64] | None = None,
    ) -> ForecastFrame:
        publication = publication or PublicationModel()
        frame = self.forecaster.predict(market, publication, rows=rows)
        cur = frame.current
        running, n_visible = running_estimate(market, self.running_field, publication)
        v_r = self.error_variance[np.clip(n_visible.astype(int), 0, MINUTES_PER_QH)]
        values = cur.values.astype(float)
        weights = cur.weights.astype(float)
        mu_dist = distribution_mean(values, weights)
        centred = values - mu_dist[:, None]
        v0 = (np.broadcast_to(weights, values.shape) * centred**2).sum(axis=1)
        mu = np.where(np.isfinite(cur.mean), cur.mean, mu_dist)
        ok = np.isfinite(running) & np.isfinite(v_r) & (v_r > 0) & np.isfinite(mu) & (v0 > 1e-9)
        safe_v0 = np.where(ok, v0, 1.0)
        safe_vr = np.where(ok, v_r, 1.0)
        post_var = 1.0 / (1.0 / safe_v0 + 1.0 / safe_vr)
        post_mean = post_var * (mu / safe_v0 + np.nan_to_num(running) / safe_vr)
        scale = np.sqrt(post_var / safe_v0)
        fused_values = post_mean[:, None] + centred * scale[:, None]
        new_values = np.where(ok[:, None], fused_values, values).astype(np.float32)
        new_mean = np.where(ok, post_mean, cur.mean)
        fused = DiscreteForecast(values=new_values, weights=cur.weights, mean=new_mean, p_short=cur.p_short)
        meta = dict(frame.meta)
        meta["fusion"] = {"running_field": self.running_field, "fused_share": float(np.mean(ok))}
        return replace(frame, current=fused, meta=meta)
