"""Synthetic Belgian real-time market with the same field names and minute semantics as the public data set.

Used by unit tests and documentation examples so the forecasting and strategy layers can be exercised without the
full ``data/public`` tree. The generator mimics the structure that matters for real-time trading, not the levels:

- an instantaneous minute system imbalance (ODS126-like ``si_mw``) built from a persistent quarter-hour component,
  a daily profile and minute noise;
- quarter-hour-to-date cumulative fields (ODS133/ODS165-like ``si_cum_mw``, ``mip_eur_mwh``, ``mdp_eur_mwh``,
  ``alpha_eur_mwh``, ``price_cum_eur_mwh``, ``afrr_price_up_eur_mwh``) whose last minute is close to, but not
  exactly, the validated quarter-hour value (as observed on Elia data);
- the current settlement price (marginal price +/- alpha) and the new 2026 design (volume-weighted product prices),
  with ``nf_`` minute fields that are also cumulative quarter-hour-to-date values;
- Elia-style SI forecasts for the current and the next quarter-hour and an hourly day-ahead price.

Nothing here is calibrated to Elia data; numbers produced on synthetic markets are for tests only.
"""

from __future__ import annotations

import numpy as np
import pandas as pd
from scipy.stats import norm

from ..mechanisms import CurrentAlphaMechanism, NewWeightedMechanism2026
from ..replay import MarketData
from ..types import MINUTES_PER_QH

_BIN_EDGES = (-np.inf, -400.0, -200.0, 0.0, 200.0, 400.0, np.inf)
_BIN_NAMES = ("p_si_lt_m400", "p_si_m400_m200", "p_si_m200_0", "p_si_0_200", "p_si_200_400", "p_si_gt_400")


def _qh_cummean(values: np.ndarray) -> np.ndarray:
    by_qh = values.reshape(-1, MINUTES_PER_QH)
    counts = np.arange(1, MINUTES_PER_QH + 1, dtype=float)
    return (np.cumsum(by_qh, axis=1) / counts).reshape(-1)


def _qh_cummax(values: np.ndarray) -> np.ndarray:
    return np.maximum.accumulate(values.reshape(-1, MINUTES_PER_QH), axis=1).reshape(-1)


def _qh_cummin(values: np.ndarray) -> np.ndarray:
    return np.minimum.accumulate(values.reshape(-1, MINUTES_PER_QH), axis=1).reshape(-1)


def _qh_vwap(price: np.ndarray, volume: np.ndarray) -> np.ndarray:
    p = price.reshape(-1, MINUTES_PER_QH)
    v = volume.reshape(-1, MINUTES_PER_QH)
    num = np.cumsum(p * v, axis=1)
    den = np.cumsum(v, axis=1)
    with np.errstate(invalid="ignore", divide="ignore"):
        out = np.where(den > 1e-9, num / np.maximum(den, 1e-9), np.cumsum(p, axis=1) / np.arange(1, 16))
    return out.reshape(-1)


def _bins(mean: np.ndarray, std: np.ndarray) -> dict[str, np.ndarray]:
    cdf = [norm.cdf((edge - mean) / std) if np.isfinite(edge) else (0.0 if edge < 0 else 1.0) for edge in _BIN_EDGES]
    return {name: np.asarray(cdf[i + 1] - cdf[i], dtype=float) for i, name in enumerate(_BIN_NAMES)}


def synthetic_market(
    days: int = 14,
    *,
    start: str = "2026-03-02T00:00:00Z",
    seed: int = 0,
    include_new_formula: bool = True,
    include_si_forecast: bool = True,
    validation_noise_mw: float = 6.0,
) -> MarketData:
    """Build a synthetic :class:`MarketData` with contract field names (see module docstring)."""
    rng = np.random.default_rng(seed)
    n_qh = days * 96
    n = n_qh * MINUTES_PER_QH
    minutes = pd.date_range(start, periods=n, freq="1min", tz="UTC")
    qh_times = pd.date_range(start, periods=n_qh, freq="15min", tz="UTC")
    hour = (minutes.hour + minutes.minute / 60.0).to_numpy(dtype=float)

    # persistent quarter-hour component (AR(1) across quarter-hours) + daily profile + minute AR(1) noise
    z = np.zeros(n_qh)
    shocks = rng.normal(0.0, 110.0, size=n_qh)
    for k in range(1, n_qh):
        z[k] = 0.82 * z[k - 1] + shocks[k]
    profile = -70.0 * np.sin(2 * np.pi * (hour - 6.0) / 24.0)
    w = np.zeros(n)
    eps = rng.normal(0.0, 22.0, size=n)
    for t in range(1, n):
        w[t] = 0.9 * w[t - 1] + eps[t]
    si_inst = np.repeat(z, MINUTES_PER_QH) + profile + w

    da_hourly = 85.0 + 35.0 * np.sin(2 * np.pi * (np.arange(days * 24) % 24 - 8.0) / 24.0)
    da_hourly = da_hourly + rng.normal(0.0, 8.0, size=len(da_hourly))
    da_minute = np.repeat(da_hourly, 60)

    short_mw = np.clip(-si_inst, 0.0, None)
    long_mw = np.clip(si_inst, 0.0, None)
    afrr_up = np.minimum(short_mw * 0.9, 250.0)
    afrr_down = np.minimum(long_mw * 0.9, 250.0)
    mfrr_up = np.clip(short_mw * 0.9 - 250.0, 0.0, None)
    mfrr_down = np.clip(long_mw * 0.9 - 250.0, 0.0, None)
    igcc_up = short_mw * 0.1
    igcc_down = long_mw * 0.1

    afrr_price_inst = da_minute + 0.35 * short_mw - 0.3 * long_mw + rng.normal(0.0, 6.0, size=n)
    mfrr_up_price_inst = da_minute + 180.0 + 0.4 * short_mw
    mfrr_down_price_inst = da_minute - 120.0 - 0.3 * long_mw

    si_cum = _qh_cummean(si_inst)
    up_price_inst = np.where(mfrr_up > 0, np.maximum(afrr_price_inst, mfrr_up_price_inst), afrr_price_inst)
    down_price_inst = np.where(mfrr_down > 0, np.minimum(afrr_price_inst, mfrr_down_price_inst), afrr_price_inst)
    floor = da_minute * 0.5
    cap = da_minute * 0.5
    mip_cum = np.maximum(_qh_cummax(np.where(short_mw > 0, up_price_inst, -np.inf)), floor)
    mdp_cum = np.minimum(_qh_cummin(np.where(long_mw > 0, down_price_inst, np.inf)), cap)
    alpha_cum = np.where(np.abs(si_cum) > 150.0, 0.12 * (np.abs(si_cum) - 150.0), 0.0)
    price_cum = CurrentAlphaMechanism.price_given_si(si_cum, mip=mip_cum, mdp=mdp_cum, alpha=alpha_cum)
    afrr_price_cum = _qh_cummean(afrr_price_inst)

    afrr_up_cum = _qh_cummean(afrr_up)
    afrr_down_cum = _qh_cummean(afrr_down)
    mfrr_up_cum = _qh_cummean(mfrr_up)
    mfrr_down_cum = _qh_cummean(mfrr_down)
    up_vwap_afrr = _qh_vwap(afrr_price_inst, afrr_up)
    down_vwap_afrr = _qh_vwap(afrr_price_inst, afrr_down)
    mfrr_up_cum_price = _qh_cummax(np.where(mfrr_up > 0, mfrr_up_price_inst, -np.inf))
    mfrr_down_cum_price = _qh_cummin(np.where(mfrr_down > 0, mfrr_down_price_inst, np.inf))
    mfrr_up_cum_price = np.where(np.isfinite(mfrr_up_cum_price), mfrr_up_cum_price, np.nan)
    mfrr_down_cum_price = np.where(np.isfinite(mfrr_down_cum_price), mfrr_down_cum_price, np.nan)
    tot_up = afrr_up_cum + mfrr_up_cum
    tot_down = afrr_down_cum + mfrr_down_cum
    with np.errstate(invalid="ignore", divide="ignore"):
        w_up = np.where(tot_up > 0, afrr_up_cum / np.maximum(tot_up, 1e-9), 1.0)
        w_down = np.where(tot_down > 0, afrr_down_cum / np.maximum(tot_down, 1e-9), 1.0)
    main_up = w_up * up_vwap_afrr + (1.0 - w_up) * np.nan_to_num(mfrr_up_cum_price)
    main_down = w_down * down_vwap_afrr + (1.0 - w_down) * np.nan_to_num(mfrr_down_cum_price)
    nf_price_cum = NewWeightedMechanism2026.price_given_si(si_cum, main_up=main_up, main_down=main_down)

    last = np.arange(n_qh) * MINUTES_PER_QH + MINUTES_PER_QH - 1
    si_final = si_cum[last] + rng.normal(0.0, validation_noise_mw, size=n_qh)
    mip_q, mdp_q, alpha_q = mip_cum[last], mdp_cum[last], alpha_cum[last]
    price_q = CurrentAlphaMechanism.price_given_si(si_final, mip=mip_q, mdp=mdp_q, alpha=alpha_q)
    nf_price_q = NewWeightedMechanism2026.price_given_si(si_final, main_up=main_up[last], main_down=main_down[last])

    minute_frame = pd.DataFrame(
        {
            "timestamp_utc": minutes,
            "si_cum_mw": si_cum,
            "alpha_eur_mwh": alpha_cum,
            "mip_eur_mwh": mip_cum,
            "mdp_eur_mwh": mdp_cum,
            "price_cum_eur_mwh": price_cum,
            "si_mw": si_inst,
            "ace_mw": si_inst * 0.3,
            "afrr_up_mw": afrr_up,
            "afrr_down_mw": afrr_down,
            "mfrr_sa_up_mw": mfrr_up,
            "mfrr_da_up_mw": np.zeros(n),
            "mfrr_sa_down_mw": mfrr_down,
            "mfrr_da_down_mw": np.zeros(n),
            "igcc_up_mw": igcc_up,
            "igcc_down_mw": igcc_down,
            "afrr_price_up_eur_mwh": afrr_price_cum,
            "afrr_price_down_eur_mwh": afrr_price_cum,
            "floor_price_eur_mwh": floor,
            "cap_price_eur_mwh": cap,
        }
    )
    qh_frame = pd.DataFrame(
        {
            "timestamp_utc": qh_times,
            "si_mw": si_final,
            "alpha_eur_mwh": alpha_q,
            "mip_eur_mwh": mip_q,
            "mdp_eur_mwh": mdp_q,
            "imbalance_price_eur_mwh": price_q,
            "afrr_price_up_eur_mwh": afrr_price_cum[last],
            "afrr_price_down_eur_mwh": afrr_price_cum[last],
            "mfrr_sa_price_up_eur_mwh": mfrr_up_cum_price[last],
            "mfrr_da_price_up_eur_mwh": np.full(n_qh, np.nan),
            "mfrr_sa_price_down_eur_mwh": mfrr_down_cum_price[last],
            "mfrr_da_price_down_eur_mwh": np.full(n_qh, np.nan),
            "afrr_up_mw": afrr_up_cum[last],
            "afrr_down_mw": afrr_down_cum[last],
            "mfrr_sa_up_mw": mfrr_up_cum[last],
            "mfrr_da_up_mw": np.zeros(n_qh),
            "mfrr_sa_down_mw": mfrr_down_cum[last],
            "mfrr_da_down_mw": np.zeros(n_qh),
        }
    )
    minute_frames: dict[str, pd.DataFrame] = {"": minute_frame}
    qh_frames: dict[str, pd.DataFrame] = {"": qh_frame}
    if include_new_formula:
        minute_frames["nf_"] = pd.DataFrame(
            {
                "timestamp_utc": minutes,
                "si_mw": si_cum,
                "imbalance_price_eur_mwh": nf_price_cum,
                "main_up_eur_mwh": main_up,
                "main_down_eur_mwh": main_down,
                "afrr_price_up_eur_mwh": up_vwap_afrr,
                "afrr_price_down_eur_mwh": down_vwap_afrr,
                "weight_afrr": np.where(si_cum < 0, w_up, w_down),
                "afrr_up_mw": afrr_up_cum,
                "afrr_down_mw": afrr_down_cum,
            }
        )
        qh_frames["nf_"] = pd.DataFrame(
            {
                "timestamp_utc": qh_times,
                "si_mw": si_final,
                "imbalance_price_eur_mwh": nf_price_q,
                "main_up_eur_mwh": main_up[last],
                "main_down_eur_mwh": main_down[last],
                "afrr_price_up_eur_mwh": up_vwap_afrr[last],
                "afrr_price_down_eur_mwh": down_vwap_afrr[last],
                "weight_afrr": np.where(si_final < 0, w_up[last], w_down[last]),
                "afrr_up_mw": afrr_up_cum[last],
                "afrr_down_mw": afrr_down_cum[last],
            }
        )
    si_frames: dict[str, pd.DataFrame] = {}
    if include_si_forecast:
        qh_of_minute = np.arange(n) // MINUTES_PER_QH
        m = np.arange(n) % MINUTES_PER_QH
        std_cur = 30.0 + 110.0 * (1.0 - m / 14.0)
        cur_mean = si_final[qh_of_minute] + rng.normal(0.0, 1.0, size=n) * std_cur
        nxt = np.minimum(qh_of_minute + 1, n_qh - 1)
        std_next = np.full(n, 150.0)
        next_mean = si_final[nxt] + rng.normal(0.0, 1.0, size=n) * std_next
        prediction_times = minutes + pd.Timedelta(seconds=59)
        si_frames["current_qh"] = pd.DataFrame(
            {
                "prediction_time_utc": prediction_times,
                "target_qh_utc": qh_times[qh_of_minute],
                "si_forecast_mw": cur_mean,
                **_bins(cur_mean, std_cur),
            }
        )
        si_frames["next_qh"] = pd.DataFrame(
            {
                "prediction_time_utc": prediction_times,
                "target_qh_utc": qh_times[nxt],
                "si_forecast_mw": next_mean,
                **_bins(next_mean, std_next),
            }
        )
    da_frame = pd.DataFrame(
        {
            "timestamp_utc": pd.date_range(start, periods=days * 24, freq="1h", tz="UTC"),
            "resolution_minutes": 60,
            "price_eur_mwh": da_hourly,
        }
    )
    return MarketData.from_frames(
        start=minutes[0],
        end=minutes[-1] + pd.Timedelta(minutes=1),
        minute_frames=minute_frames,
        qh_frames=qh_frames,
        da_frame=da_frame,
        si_forecast_frames=si_frames,
        meta={"source": "synthetic", "seed": seed},
    )
