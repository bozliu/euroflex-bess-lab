"""Synthetic Belgian market with a known price response (tests, examples, docs).

The generator draws a persistent system imbalance, gross aFRR / mFRR activations consistent with it, marginal and
product prices that rise / fall *linearly* with the activated volume (known slopes), the tariff alpha, and both
settlement prices (current and 2026 formula) computed with the real mechanism operators. Running (QH-to-date)
minute estimates converge to the final quarter-hour values, so strategies see realistic intra-quarter-hour noise.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from ..mechanisms import CurrentAlphaMechanism, NewWeightedMechanism2026
from ..replay import MarketData
from .alpha import TARIFF_ALPHA


def synthetic_impact_market(
    *,
    days: int = 3,
    start: str = "2026-03-02T00:00:00Z",
    seed: int = 0,
    mip_slope: float = 0.15,
    mdp_slope: float = 0.12,
    afrr_slope: float = 0.10,
    si_sd_mw: float = 220.0,
    include_new_formula: bool = True,
) -> MarketData:
    """Build a :class:`MarketData` with current and (optionally) 2026-formula fields and settlement prices."""
    rng = np.random.default_rng(seed)
    n = days * 96
    qh_times = pd.date_range(start, periods=n, freq="15min", tz="UTC")
    minutes = pd.date_range(start, periods=n * 15, freq="1min", tz="UTC")
    # persistent imbalance (AR(1)) with occasional large swings
    si = np.zeros(n)
    eps = rng.normal(0.0, si_sd_mw * np.sqrt(1 - 0.6**2), n)
    for k in range(1, n):
        si[k] = 0.6 * si[k - 1] + eps[k]
    up = np.clip(-si, 0.0, None) * 0.95 + rng.uniform(0.0, 25.0, n)
    down = np.clip(si, 0.0, None) * 0.95 + rng.uniform(0.0, 25.0, n)
    hours = np.arange(n) / 4.0
    level = 85.0 + 20.0 * np.sin(2.0 * np.pi * hours / 24.0)
    mip = level + 10.0 + mip_slope * up + rng.normal(0.0, 4.0, n)
    mdp = level - 30.0 - mdp_slope * down + rng.normal(0.0, 4.0, n)
    si_prev = np.concatenate([[si[0]], si[:-1]])
    alpha = TARIFF_ALPHA.predict(si, si_prev, mip=mip, mdp=mdp)  # Elia's rule incl. the price factor
    price = CurrentAlphaMechanism.price_given_si(si, mip=mip, mdp=mdp, alpha=alpha)
    afrr_share = 0.8
    qh = pd.DataFrame(
        {
            "timestamp_utc": qh_times,
            "si_mw": si,
            "mip_eur_mwh": mip,
            "mdp_eur_mwh": mdp,
            "alpha_eur_mwh": alpha,
            "imbalance_price_eur_mwh": price,
            "afrr_up_mw": up * afrr_share,
            "mfrr_sa_up_mw": up * (1.0 - afrr_share),
            "mfrr_da_up_mw": np.zeros(n),
            "afrr_down_mw": down * afrr_share,
            "mfrr_sa_down_mw": down * (1.0 - afrr_share),
            "mfrr_da_down_mw": np.zeros(n),
        }
    )
    # running QH-to-date minute estimates converge to the final values
    progress = (np.arange(n * 15) % 15 + 1) / 15.0
    q_of_m = np.arange(n * 15) // 15
    noise = rng.normal(0.0, 1.0, n * 15) * (1.0 - progress)
    minute = pd.DataFrame(
        {
            "timestamp_utc": minutes,
            "si_cum_mw": si[q_of_m] + 60.0 * noise,
            "mip_eur_mwh": mip[q_of_m] + 6.0 * noise,
            "mdp_eur_mwh": mdp[q_of_m] + 6.0 * noise,
            "alpha_eur_mwh": alpha[q_of_m],
            "price_cum_eur_mwh": price[q_of_m] + 15.0 * noise,
        }
    )
    qh_frames = {"": qh}
    minute_frames = {"": minute}
    if include_new_formula:
        a_up, a_dn = up * afrr_share, down * afrr_share
        m_up, m_dn = up * (1.0 - afrr_share), down * (1.0 - afrr_share)
        pa_up = level + 5.0 + afrr_slope * a_up + rng.normal(0.0, 3.0, n)
        pa_dn = level - 15.0 - afrr_slope * a_dn + rng.normal(0.0, 3.0, n)
        pm_up = level + 60.0 + rng.normal(0.0, 5.0, n)
        pm_dn = level - 60.0 + rng.normal(0.0, 5.0, n)
        net_a, net_m = np.abs(a_up - a_dn), np.abs(m_up - m_dn)
        w_a = np.where(net_a + net_m > 0, net_a / np.maximum(net_a + net_m, 1e-9), 1.0)
        w_m = 1.0 - w_a
        main_up = w_a * pa_up + w_m * pm_up
        main_dn = w_a * pa_dn + w_m * pm_dn
        incentive = np.full(n, np.nan)
        nf_price = NewWeightedMechanism2026.price_given_si(si, main_up=main_up, main_down=main_dn, incentive=incentive)
        nf = pd.DataFrame(
            {
                "timestamp_utc": qh_times,
                "si_mw": si,
                "imbalance_price_eur_mwh": nf_price,
                "main_up_eur_mwh": main_up,
                "main_down_eur_mwh": main_dn,
                "incentive_eur_mwh": incentive,
                "afrr_price_up_eur_mwh": pa_up,
                "afrr_price_down_eur_mwh": pa_dn,
                "mfrr_price_up_eur_mwh": pm_up,
                "mfrr_price_down_eur_mwh": pm_dn,
                "weight_afrr": w_a,
                "weight_mfrr": w_m,
                "afrr_up_mw": a_up,
                "afrr_down_mw": a_dn,
                "mfrr_sa_up_mw": m_up,
                "mfrr_sa_down_mw": m_dn,
                "mfrr_da_up_mw": np.zeros(n),
                "mfrr_da_down_mw": np.zeros(n),
            }
        )
        qh_frames["nf_"] = nf
        nf_min = pd.DataFrame({"timestamp_utc": minutes})
        for column in nf.columns:
            if column == "timestamp_utc":
                continue
            values = nf[column].to_numpy(dtype=float)[q_of_m]
            if column in ("si_mw",):
                values = values + 60.0 * noise
            elif column.endswith("_eur_mwh"):
                values = values + 10.0 * noise
            nf_min[column] = values
        minute_frames["nf_"] = nf_min
    return MarketData.from_frames(
        start=qh_times[0],
        end=qh_times[-1] + pd.Timedelta(minutes=15),
        minute_frames=minute_frames,
        qh_frames=qh_frames,
        meta={"source": "synthetic_impact_market", "seed": seed},
    )
