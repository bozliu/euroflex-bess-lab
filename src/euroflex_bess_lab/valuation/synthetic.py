"""SYNTHETIC ledgers for tests, demos and documentation - not market data and not calibrated to any market.

Seeded quarter-hour price paths with imbalance-price-like features (seasonal level, intraday shape, volatility
clusters, fat-tailed noise, rare spikes) feed a *toy* daily-arbitrage dispatch, so that oracle / strategy /
mechanism / impact / availability / degradation ledgers share one construction and common random numbers. The toy
dispatch ignores the intraday ordering of charge and discharge (no state-of-charge trajectory); it exists only to
exercise the valuation chain end to end.
"""

from __future__ import annotations

import math
from dataclasses import dataclass

import numpy as np
import pandas as pd

from .revenue import LedgerMetadata

SYNTHETIC_LABEL = "SYNTHETIC (seeded toy generator, not market data)"
QH_PER_DAY = 96


@dataclass(frozen=True)
class SyntheticMarket:
    """Parameters of one synthetic price process (EUR/MWh)."""

    zone: str = "BE"
    level_eur_mwh: float = 85.0
    seasonal_amplitude: float = 0.25
    volatility_eur_mwh: float = 75.0
    volatility_seasonality: float = 0.25
    factor_loading: float = 0.7
    intraday_ar: float = 0.7
    spike_probability: float = 0.004
    spike_scale_eur_mwh: float = 350.0
    hourly_blocks: bool = False


BELGIUM_IMBALANCE_LIKE = SyntheticMarket()
GERMANY_DAY_AHEAD_LIKE = SyntheticMarket(
    zone="DE-LU",
    level_eur_mwh=80.0,
    seasonal_amplitude=0.2,
    volatility_eur_mwh=28.0,
    volatility_seasonality=0.3,
    factor_loading=0.6,
    intraday_ar=0.85,
    spike_probability=0.0005,
    spike_scale_eur_mwh=120.0,
    hourly_blocks=True,
)


def _utc_days(start: str, end: str) -> pd.DatetimeIndex:
    days = pd.date_range(pd.Timestamp(start, tz="UTC"), pd.Timestamp(end, tz="UTC"), freq="D", inclusive="left")
    if len(days) == 0:
        raise ValueError("synthetic period must span at least one day")
    return days


def synthetic_prices(
    start: str = "2024-07-01",
    end: str = "2026-07-01",
    *,
    market: SyntheticMarket = BELGIUM_IMBALANCE_LIKE,
    seed: int = 7,
    factor_seed: int = 11,
    new_formula_compression: float = 0.8,
    incentive_probability: float = 0.0015,
    incentive_scale_eur_mwh: float = 250.0,
) -> pd.DataFrame:
    """Quarter-hour synthetic prices on a UTC grid ``[start, end)``.

    Columns: ``timestamp_utc``, ``price_eur_mwh`` (current-mechanism analogue), ``price_new2026_eur_mwh`` (a
    *synthetic* new-mechanism analogue: deviations from the smooth level compressed by ``new_formula_compression``
    plus rare incentive spikes) and ``base_eur_mwh`` (the smooth level + intraday shape). ``factor_seed`` drives the
    daily volatility factor shared across markets, which correlates their P&L.
    """
    days = _utc_days(start, end)
    n_days = len(days)
    rng = np.random.default_rng(seed)
    factor_rng = np.random.default_rng(factor_seed)
    doy = np.asarray(days.day_of_year, dtype=float)
    season = np.cos(2.0 * math.pi * (doy - 15.0) / 365.25)

    common = np.zeros(n_days)
    idio = np.zeros(n_days)
    shocks_common = factor_rng.standard_normal(n_days)
    shocks_idio = rng.standard_normal(n_days)
    for d in range(1, n_days):
        common[d] = 0.85 * common[d - 1] + math.sqrt(1 - 0.85**2) * shocks_common[d]
        idio[d] = 0.85 * idio[d - 1] + math.sqrt(1 - 0.85**2) * shocks_idio[d]
    loading = market.factor_loading
    vol_factor = loading * common + math.sqrt(1.0 - loading**2) * idio
    sigma = (
        market.volatility_eur_mwh * np.exp(0.35 * vol_factor - 0.06) * (1.0 + market.volatility_seasonality * season)
    )
    level_shock = np.zeros(n_days)
    level_innov = rng.standard_normal(n_days) * 8.0
    for d in range(1, n_days):
        level_shock[d] = 0.9 * level_shock[d - 1] + level_innov[d]
    level = market.level_eur_mwh * (1.0 + market.seasonal_amplitude * season) + level_shock

    hours = (np.arange(QH_PER_DAY) + 0.5) / 4.0
    peaks = 18.0 * np.exp(-(((hours - 8.0) / 2.0) ** 2)) + 22.0 * np.exp(-(((hours - 19.0) / 2.5) ** 2))
    solar = np.exp(-(((hours - 13.0) / 3.0) ** 2))
    summer = (1.0 - season) / 2.0
    shape = peaks[None, :] - 35.0 * summer[:, None] * solar[None, :]
    base = level[:, None] + shape

    innovations = rng.standard_t(4.0, size=(n_days, QH_PER_DAY)) / math.sqrt(2.0)
    noise = np.zeros((n_days, QH_PER_DAY))
    phi = market.intraday_ar
    noise[:, 0] = innovations[:, 0]
    for q in range(1, QH_PER_DAY):
        noise[:, q] = phi * noise[:, q - 1] + math.sqrt(1.0 - phi**2) * innovations[:, q]
    deviation = sigma[:, None] * noise
    spikes = rng.random((n_days, QH_PER_DAY)) < market.spike_probability
    spike_sign = np.where(rng.random((n_days, QH_PER_DAY)) < 0.7, 1.0, -1.0)
    deviation += spikes * spike_sign * rng.exponential(market.spike_scale_eur_mwh, size=(n_days, QH_PER_DAY))
    if market.hourly_blocks:
        deviation = np.repeat(deviation.reshape(n_days, QH_PER_DAY // 4, 4).mean(axis=2), 4, axis=1)
        base = np.repeat(base.reshape(n_days, QH_PER_DAY // 4, 4).mean(axis=2), 4, axis=1)
    price = base + deviation

    incentive = rng.random((n_days, QH_PER_DAY)) < incentive_probability
    incentive_sign = np.sign(deviation + 1e-9)
    new_price = base + new_formula_compression * deviation
    new_price += incentive * incentive_sign * rng.exponential(incentive_scale_eur_mwh, size=(n_days, QH_PER_DAY))

    timestamps = pd.date_range(days[0], periods=n_days * QH_PER_DAY, freq="15min")
    return pd.DataFrame(
        {
            "timestamp_utc": timestamps,
            "price_eur_mwh": price.ravel(),
            "price_new2026_eur_mwh": new_price.ravel(),
            "base_eur_mwh": base.ravel(),
        }
    )


def synthetic_ledger(
    prices: pd.DataFrame,
    *,
    price_column: str = "price_eur_mwh",
    power_mw: float = 1.0,
    energy_mwh: float = 2.0,
    round_trip_efficiency: float = 0.88,
    usable_fraction: float = 0.9,
    max_cycles_per_day: float = 1.5,
    degradation_eur_per_mwh: float = 8.0,
    forecast_noise_eur_mwh: float = 0.0,
    noise_seed: int = 101,
    fleet_mw: float = 0.0,
    impact_eur_mwh_per_mw: float = 0.0,
    impact_aware: bool = False,
    availability: float = 1.0,
    outage_seed: int = 202,
) -> pd.DataFrame:
    """Toy daily arbitrage on synthetic prices -> canonical quarter-hour ledger.

    Each UTC day the battery pairs its highest-forecast and lowest-forecast quarter-hours (up to the cycle limit)
    and trades a pair when the forecast spread covers efficiency and wear. With ``fleet_mw`` > 0 the fleet herds
    with the battery and moves the settlement price against it by ``impact_eur_mwh_per_mw`` per MW of fleet
    deviation; ``impact_aware`` makes the dispatch anticipate that. Outages remove whole days with probability
    ``1 - availability``.
    """
    frame = prices.loc[:, ["timestamp_utc", price_column]].copy()
    n = len(frame)
    if n % QH_PER_DAY:
        raise ValueError("synthetic ledger needs whole UTC days of quarter-hours")
    n_days = n // QH_PER_DAY
    price = frame[price_column].to_numpy(dtype=float).reshape(n_days, QH_PER_DAY)
    noise = np.random.default_rng(noise_seed).standard_normal((n_days, QH_PER_DAY)) * forecast_noise_eur_mwh
    forecast = price + noise
    qh_energy = 0.25 * power_mw
    n_pairs = int(min(QH_PER_DAY // 2, math.floor(max_cycles_per_day * energy_mwh * usable_fraction / qh_energy)))
    discharge_energy = qh_energy * round_trip_efficiency
    charge_energy = qh_energy
    impact = impact_eur_mwh_per_mw * fleet_mw
    discharge_impact = impact * round_trip_efficiency
    order = np.argsort(forecast, axis=1, kind="stable")
    high = order[:, ::-1][:, :n_pairs]
    low = order[:, :n_pairs]
    rows = np.arange(n_days)[:, None]
    f_high = forecast[rows, high]
    f_low = forecast[rows, low]
    expected_impact = (discharge_impact * discharge_energy + impact * charge_energy) if impact_aware else 0.0
    margin = f_high * discharge_energy - f_low * charge_energy - degradation_eur_per_mwh * discharge_energy
    trade = margin - expected_impact > 0.0
    energy = np.zeros((n_days, QH_PER_DAY))
    energy[rows, high] = np.where(trade, discharge_energy, 0.0)
    energy[rows, low] = np.where(trade, -charge_energy, energy[rows, low])
    if availability < 1.0:
        up = np.random.default_rng(outage_seed).random(n_days) < availability
        energy *= up[:, None]
    direction = energy / qh_energy
    settlement_price = price - impact * direction
    revenue = energy * settlement_price
    discharged = np.clip(energy, 0.0, None)
    charged = np.clip(-energy, 0.0, None)
    degradation = discharged * degradation_eur_per_mwh
    return pd.DataFrame(
        {
            "timestamp_utc": frame["timestamp_utc"].to_numpy(),
            "energy_mwh": energy.ravel(),
            "discharged_mwh": discharged.ravel(),
            "charged_mwh": charged.ravel(),
            "price_eur_mwh": settlement_price.ravel(),
            "price_missing": np.zeros(n, dtype=bool),
            "revenue_eur": revenue.ravel(),
            "degradation_eur": degradation.ravel(),
            "pnl_eur": (revenue - degradation).ravel(),
        }
    )


def synthetic_ledger_set(
    start: str = "2024-07-01",
    end: str = "2026-07-01",
    *,
    seed: int = 7,
    power_mw: float = 1.0,
    energy_mwh: float = 2.0,
    strategy_noise_eur_mwh: float = 45.0,
    index_noise_eur_mwh: float = 110.0,
    fleet_mw: float = 1500.0,
    impact_eur_mwh_per_mw: float = 0.012,
    availability: float = 0.97,
    max_cycles_per_day: float = 1.5,
    cycle_cap_per_day: float = 1.0,
    degradation_eur_per_mwh: float = 8.0,
) -> dict[str, tuple[pd.DataFrame, LedgerMetadata]]:
    """A full synthetic chain for one Belgian-like battery, a benchmark index and a German-like asset.

    Keys ``oracle``, ``strategy``, ``mechanism``, ``impact``, ``availability``, ``degradation`` form the attribution
    chain (each adds one factor to the previous ledger); ``index`` is a cruder benchmark strategy on the same prices
    (reference for swaps and the skill share); ``germany`` is a day-ahead-like arbitrage on a second market whose
    volatility factor is shared with the Belgian one. All ledgers use common random numbers.
    """
    be = synthetic_prices(start, end, market=BELGIUM_IMBALANCE_LIKE, seed=seed, factor_seed=seed + 4)
    de = synthetic_prices(start, end, market=GERMANY_DAY_AHEAD_LIKE, seed=seed + 1, factor_seed=seed + 4)

    def run(
        prices: pd.DataFrame,
        *,
        price_column: str = "price_eur_mwh",
        noise: float = 0.0,
        cycles: float = max_cycles_per_day,
        fleet: float = 0.0,
        avail: float = 1.0,
    ) -> pd.DataFrame:
        return synthetic_ledger(
            prices,
            price_column=price_column,
            power_mw=power_mw,
            energy_mwh=energy_mwh,
            max_cycles_per_day=cycles,
            degradation_eur_per_mwh=degradation_eur_per_mwh,
            forecast_noise_eur_mwh=noise,
            noise_seed=seed + 100,
            fleet_mw=fleet,
            impact_eur_mwh_per_mw=impact_eur_mwh_per_mw if fleet else 0.0,
            impact_aware=fleet > 0.0,
            availability=avail,
            outage_seed=seed + 200,
        )

    def meta(asset_id: str, **fields: object) -> LedgerMetadata:
        values: dict[str, object] = {
            "asset_id": asset_id,
            "market": "BE",
            "power_mw": power_mw,
            "energy_mwh": energy_mwh,
            "degradation_eur_per_mwh": degradation_eur_per_mwh,
            "publication_lag": "n/a (synthetic toy dispatch with a noisy day-level forecast)",
            "period_start": start,
            "period_end": end,
            "source": SYNTHETIC_LABEL,
            "synthetic": True,
        }
        values.update(fields)
        return LedgerMetadata.from_dict(values)

    new = "price_new2026_eur_mwh"
    noisy = strategy_noise_eur_mwh
    impact_meta: dict[str, object] = {"mechanism": "new2026", "fleet_mw": fleet_mw, "impact_aware": True}
    aware = "toy forecast strategy (impact-aware)"
    return {
        "oracle": (run(be), meta("be_oracle", mechanism="current", strategy="oracle (perfect foresight)")),
        "strategy": (
            run(be, noise=noisy),
            meta("be_strategy_current", mechanism="current", strategy="toy forecast strategy"),
        ),
        "mechanism": (
            run(be, price_column=new, noise=noisy),
            meta("be_strategy_new2026", mechanism="new2026", strategy="toy forecast strategy"),
        ),
        "impact": (
            run(be, price_column=new, noise=noisy, fleet=fleet_mw),
            meta("be_strategy_new2026_impact", strategy=aware, **impact_meta),
        ),
        "availability": (
            run(be, price_column=new, noise=noisy, fleet=fleet_mw, avail=availability),
            meta("be_strategy_new2026_impact_avail", strategy=aware, availability=availability, **impact_meta),
        ),
        "degradation": (
            run(be, price_column=new, noise=noisy, fleet=fleet_mw, avail=availability, cycles=cycle_cap_per_day),
            meta(
                "be_strategy_new2026_impact_avail_cyclecap",
                strategy=f"{aware}, {cycle_cap_per_day:g} cycle/day cap",
                availability=availability,
                **impact_meta,
            ),
        ),
        "index": (
            run(be, noise=index_noise_eur_mwh),
            meta("be_index_current", mechanism="current", strategy="toy benchmark index (crude forecast)"),
        ),
        "germany": (
            run(de, cycles=1.0),
            meta("de_day_ahead", market="DE-LU", strategy="toy day-ahead arbitrage"),
        ),
    }
