"""Forecast skill per minute-in-quarter-hour: pinball loss, CRPS, MAE, interval coverage and Brier score."""

from __future__ import annotations

from collections.abc import Sequence

import numpy as np
import pandas as pd
from numpy.typing import NDArray

from ..replay import MarketData
from ..types import MINUTES_PER_QH, PublicationModel
from .base import DiscreteForecast
from .distribution import crps_discrete, weighted_quantile
from .features import _ffill_within_qh, _Index, _take, minute_target
from .models import DEFAULT_QUANTILES

FloatArray = NDArray[np.float64]


def pinball_loss(y: FloatArray, q: FloatArray, tau: float) -> FloatArray:
    diff = np.asarray(y, dtype=float) - np.asarray(q, dtype=float)
    return np.maximum(tau * diff, (tau - 1.0) * diff)


def baseline_point_forecasts(
    market: MarketData,
    prices_qh: FloatArray,
    publication: PublicationModel,
    running_field: str | None,
    horizon: int = 0,
) -> dict[str, FloatArray]:
    """Naive point forecasts: last published final price (persistence) and the running estimate (horizon 0)."""
    ix = _Index.build(market.n_minutes, publication)
    persistence = _take(np.asarray(prices_qh, dtype=float), ix.k_last, ix.k_last_valid)
    out = {"persistence": persistence}
    if horizon == 0 and running_field is not None and running_field in market.minute:
        raw = _ffill_within_qh(np.asarray(market.minute[running_field], dtype=float), 2)
        running = _take(raw, ix.r, ix.in_qh)
        out["running"] = np.where(np.isnan(running), persistence, running)
    return out


def forecast_skill(
    forecast: DiscreteForecast,
    market: MarketData,
    prices_qh: FloatArray,
    *,
    si_qh: FloatArray | None = None,
    horizon: int = 0,
    rows: NDArray[np.int64] | None = None,
    levels: Sequence[float] = DEFAULT_QUANTILES,
    publication: PublicationModel | None = None,
    running_field: str | None = None,
) -> pd.DataFrame:
    """Skill table indexed by minute-in-quarter-hour (plus an ``all`` row) on rows with a forecast and a target."""
    publication = publication or PublicationModel()
    n = market.n_minutes
    rows = np.arange(n) if rows is None else np.asarray(rows)
    y_all = minute_target(market, prices_qh, horizon)
    ok = forecast.available()[rows] & np.isfinite(y_all[rows])
    rows = rows[ok]
    y = y_all[rows]
    values = forecast.values[rows].astype(float)
    weights = forecast.weights[rows].astype(float)
    mean = forecast.mean[rows]
    quantiles = {tau: weighted_quantile(values, weights, tau) for tau in levels}
    pinball = np.mean([pinball_loss(y, quantiles[tau], tau) for tau in levels], axis=0)
    crps = crps_discrete(values, weights, y)
    lo, hi = min(levels), max(levels)
    covered = (y >= quantiles[lo]) & (y <= quantiles[hi])
    frame = pd.DataFrame(
        {
            "minute_in_qh": rows % MINUTES_PER_QH,
            "pinball": pinball,
            "crps": crps,
            "abs_err_mean": np.abs(mean - y),
            "sq_err_mean": (mean - y) ** 2,
            "abs_err_median": np.abs(quantiles[0.5] - y) if 0.5 in quantiles else np.nan,
            "covered": covered.astype(float),
        }
    )
    baselines = baseline_point_forecasts(market, prices_qh, publication, running_field, horizon)
    for name, values_b in baselines.items():
        frame[f"abs_err_{name}"] = np.abs(values_b[rows] - y)
    if si_qh is not None and forecast.p_short is not None:
        si_t = minute_target(market, np.asarray(si_qh, dtype=float), horizon)[rows]
        event = (si_t < 0.0).astype(float)
        p = forecast.p_short[rows]
        frame["brier"] = np.where(np.isfinite(si_t) & np.isfinite(p), (p - event) ** 2, np.nan)
        base_rate = float(np.nanmean(event)) if len(event) else np.nan
        frame["brier_climatology"] = np.where(np.isfinite(si_t), (base_rate - event) ** 2, np.nan)
    grouped = frame.groupby("minute_in_qh").mean(numeric_only=True)
    grouped["n"] = frame.groupby("minute_in_qh").size()
    overall = frame.mean(numeric_only=True).to_frame().T
    overall["n"] = len(frame)
    overall.index = pd.Index(["all"], name="minute_in_qh")
    table = pd.concat([grouped.drop(columns=["minute_in_qh"], errors="ignore"), overall.drop(columns=["minute_in_qh"])])
    table["rmse_mean"] = np.sqrt(table.pop("sq_err_mean"))
    table = table.rename(
        columns={
            "abs_err_mean": "mae_mean",
            "abs_err_median": "mae_median",
            "covered": f"coverage_{int(round((hi - lo) * 100))}",
            "abs_err_persistence": "mae_persistence",
            "abs_err_running": "mae_running",
        }
    )
    table.index = table.index.astype(str)
    table.index.name = "minute_in_qh"
    return table
