"""Small synthetic Belgian-like inputs (minute + quarter-hour market data and aFRR capacity awards) for tests/docs.

The generator mimics the post-PICASSO structure: one aFRR energy price per minute applying to both directions,
cumulative quarter-hour-to-date minute publications, upward activation when the system is short and downward
activation when it is long. Values are random but deterministic given ``seed``; they carry no market meaning.
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from ..mechanisms import CurrentAlphaMechanism
from ..replay import MarketData
from ..types import MINUTES_PER_QH
from .capacity import DeliveryBlocks
from .products import AfrrProduct


def _running_mean(values: np.ndarray) -> np.ndarray:
    by_qh = values.reshape(-1, MINUTES_PER_QH)
    counts = np.arange(1, MINUTES_PER_QH + 1, dtype=float)
    return (np.cumsum(by_qh, axis=1) / counts).reshape(-1)


def synthetic_reserve_inputs(
    *,
    start: str = "2026-03-02T00:00:00Z",
    days: int = 4,
    seed: int = 1,
    contracted_mw: float = 100.0,
    capacity_price_up: float = 30.0,
    capacity_price_down: float = 20.0,
    product: AfrrProduct | None = None,
) -> tuple[MarketData, pd.DataFrame]:
    """Return ``(market, awards_frame)`` with every field the reserve layer reads."""
    product = product or AfrrProduct()
    rng = np.random.default_rng(seed)
    n_min = days * 1440
    minutes = pd.date_range(start, periods=n_min, freq="1min", tz="UTC")
    qh_times = pd.date_range(start, periods=n_min // MINUTES_PER_QH, freq="15min", tz="UTC")
    hours = pd.date_range(start, periods=days * 24, freq="1h", tz="UTC")
    da_hourly = 70.0 + 25.0 * np.sin(np.arange(days * 24) / 24.0 * 2.0 * np.pi - 1.0) + rng.normal(0, 5, days * 24)
    da_minute = np.repeat(da_hourly, 60)

    si = np.zeros(n_min)
    shock = rng.normal(0.0, 25.0, n_min)
    for t in range(1, n_min):
        si[t] = 0.97 * si[t - 1] + shock[t]
    afrr_up = np.clip(-0.6 * si + rng.normal(0, 8, n_min), 0.0, 220.0)
    afrr_down = np.clip(0.6 * si + rng.normal(0, 8, n_min), 0.0, 220.0)
    price_minute = da_minute + 0.35 * (-si) + rng.normal(0.0, 15.0, n_min)
    price_cum = _running_mean(price_minute)
    si_cum = _running_mean(si)

    qh_price_afrr = price_minute.reshape(-1, MINUTES_PER_QH).mean(axis=1)
    si_qh = si.reshape(-1, MINUTES_PER_QH).mean(axis=1)
    alpha = np.where(np.abs(si_qh) > 150.0, 12.0, 0.0)
    mip = np.maximum(qh_price_afrr, da_hourly.repeat(4)) + 5.0
    mdp = np.minimum(qh_price_afrr, da_hourly.repeat(4)) - 5.0
    imbalance = CurrentAlphaMechanism.price_given_si(si_qh, mip=mip, mdp=mdp, alpha=alpha)
    imbalance_cum = price_cum + np.where(si_cum < 0.0, 5.0, -5.0)

    minute = pd.DataFrame(
        {
            "timestamp_utc": minutes,
            "si_mw": si,
            "si_cum_mw": si_cum,
            "price_cum_eur_mwh": imbalance_cum,
            "afrr_up_mw": afrr_up,
            "afrr_down_mw": afrr_down,
            "afrr_price_up_eur_mwh": price_cum,
            "afrr_price_down_eur_mwh": price_cum,
        }
    )
    qh = pd.DataFrame(
        {
            "timestamp_utc": qh_times,
            "si_mw": si_qh,
            "mip_eur_mwh": mip,
            "mdp_eur_mwh": mdp,
            "alpha_eur_mwh": alpha,
            "imbalance_price_eur_mwh": imbalance,
            "afrr_price_up_eur_mwh": qh_price_afrr,
            "afrr_price_down_eur_mwh": qh_price_afrr,
            "afrr_up_mw": afrr_up.reshape(-1, MINUTES_PER_QH).mean(axis=1),
            "afrr_down_mw": afrr_down.reshape(-1, MINUTES_PER_QH).mean(axis=1),
        }
    )
    da = pd.DataFrame({"timestamp_utc": hours, "resolution_minutes": 60, "price_eur_mwh": da_hourly})
    end = minutes[-1] + pd.Timedelta(minutes=1)
    market = MarketData.from_frames(start=start, end=end, minute_frames={"": minute}, qh_frames={"": qh}, da_frame=da)

    blocks = DeliveryBlocks.regular(minutes[0] - pd.Timedelta(days=8), end, product)
    rows = []
    for i in range(blocks.n):
        for direction, base in (("up", capacity_price_up), ("down", capacity_price_down)):
            vwap = max(base * (1.0 + 0.3 * np.sin(i / 3.0)) + rng.normal(0.0, 2.0), 0.5)
            rows.append(
                {
                    "delivery_start_utc": blocks.start_utc[i],
                    "delivery_end_utc": blocks.end_utc[i],
                    "direction": direction,
                    "awarded_mw": contracted_mw,
                    "vwap_eur_per_mw_h": vwap,
                    "marginal_eur_per_mw_h": vwap * 1.4,
                    "n_bids": 20,
                }
            )
    return market, pd.DataFrame(rows)
