"""Causal forecasts: day-ahead persistence (incl. DST), shrunk intraday profile, residual persistence."""

from __future__ import annotations

from dataclasses import replace
from datetime import date, timedelta

import numpy as np
import pandas as pd
import pytest

from euroflex_bess_lab.forecasts import PersistenceForecastProvider
from euroflex_bess_lab.markets.intraday import (
    ForecastSettings,
    LiveForecaster,
    fit_spread_model,
    persistence_day_ahead,
    synthetic_market,
)
from euroflex_bess_lab.markets.intraday.forecast import residual_betas, slot_profile


def _provider_frame(market: object, day: date) -> pd.DataFrame:
    lo, hi = market.day_bounds(day)  # type: ignore[attr-defined]
    stamps = market.timestamps[lo:hi]  # type: ignore[attr-defined]
    return pd.DataFrame(
        {
            "timestamp_utc": stamps,
            "timestamp_local": stamps.tz_convert("Europe/Berlin"),
            "resolution_minutes": 15,
            "price_eur_per_mwh": market.da_price[lo:hi],  # type: ignore[attr-defined]
        }
    )


@pytest.mark.parametrize("target", [date(2026, 3, 29), date(2026, 3, 30), date(2026, 10, 25), date(2026, 10, 26)])
def test_persistence_matches_the_repository_provider_across_dst(target: date) -> None:
    market = synthetic_market(target - timedelta(days=3), 5, hourly_until=None, seed=4)
    forecast, source = persistence_day_ahead(market, target)
    assert source == target - timedelta(days=1)
    lo, hi = market.day_bounds(target)
    assert len(forecast) == hi - lo
    delivery = _provider_frame(market, target)
    history = _provider_frame(market, source)
    snapshot = PersistenceForecastProvider().get_forecast(
        market="day_ahead",
        decision_time_utc=pd.Timestamp(f"{source} 10:00", tz="UTC"),
        delivery_frame=delivery,
        actual_frame=pd.concat([history, delivery], ignore_index=True),
    )
    expected = snapshot.sort_values("delivery_start_utc")["forecast_price_eur_per_mwh"].to_numpy()
    assert np.allclose(forecast, expected)


def test_persistence_never_reads_the_delivery_day() -> None:
    market = synthetic_market("2026-04-01", 4, hourly_until=None, seed=2)
    day = date(2026, 4, 3)
    before, _ = persistence_day_ahead(market, day)
    lo, hi = market.day_bounds(day)
    prices = market.da_price.copy()
    prices[lo:] = 9999.0
    after, _ = persistence_day_ahead(market.with_da_prices(prices), day)
    assert np.array_equal(before, after)


def test_soft_threshold_profile_keeps_strong_slots_and_zeroes_noise() -> None:
    rng = np.random.default_rng(0)
    days = 56
    slot = np.tile(np.arange(96), days)
    signal = np.where(slot % 4 == 3, 25.0, 0.0)  # strong pattern in every fourth slot
    spread = signal + rng.normal(0.0, 40.0, len(slot))
    profile = slot_profile(spread, slot, shrinkage="soft", threshold_se=2.0)
    strong = profile[3::4]
    noise = np.delete(profile, np.arange(3, 96, 4))
    assert (strong > 5.0).mean() > 0.8
    assert (noise == 0.0).mean() > 0.85
    raw = slot_profile(spread, slot, shrinkage="none")
    assert np.count_nonzero(raw) == 96
    js = slot_profile(spread, slot, shrinkage="js")
    assert np.all(np.abs(js) <= np.abs(raw) + 1e-12)
    with pytest.raises(ValueError, match="shrinkage"):
        slot_profile(spread, slot, shrinkage="bogus")


def test_residual_betas_follow_ar1_persistence_and_drop_noise() -> None:
    rng = np.random.default_rng(1)
    n = 96 * 56
    ar = np.zeros(n)
    eps = rng.normal(0.0, 20.0, n)
    for i in range(1, n):
        ar[i] = 0.8 * ar[i - 1] + eps[i]
    beta = residual_betas(ar, window=1, max_lead=40, min_t=2.0)
    assert 0.72 < beta[1] < 0.88 and 0.45 < beta[3] < 0.6
    assert beta[1] > beta[3] > beta[8]
    white = residual_betas(rng.normal(0.0, 20.0, n), window=2, max_lead=40, min_t=2.0)
    assert (white == 0.0).mean() > 0.85


def test_spread_model_uses_only_days_before_d_minus_1() -> None:
    market = synthetic_market("2026-02-01", 64, hourly_until=None, seed=3)
    day = market.days[-1]
    model = fit_spread_model(market, day, ForecastSettings())
    lo_prev, _ = market.day_bounds(day - timedelta(days=1))
    noisy = market.id_price.copy()
    noisy[lo_prev:] = np.random.default_rng(9).normal(0.0, 500.0, len(noisy) - lo_prev)
    model2 = fit_spread_model(market.with_id_prices(noisy), day, ForecastSettings())
    assert np.array_equal(model.profile, model2.profile)
    assert np.array_equal(model.beta, model2.beta)
    assert model.history_days == 56


def test_live_forecaster_ignores_prices_that_are_not_visible_yet() -> None:
    market = synthetic_market("2026-02-01", 62, hourly_until=None, seed=5)
    day = market.days[-1]
    lo, hi = market.day_bounds(day)
    settings = ForecastSettings()
    model = fit_spread_model(market, day, settings)
    offset = 3
    for k in (0, 10, 50, hi - lo - 1):
        live = LiveForecaster(model, market, lo, hi, offset)
        forecast = live.forecast(k)
        hidden = market.id_price.copy()
        hidden[live.visible_end(k) :] = 1e4  # everything not yet visible for product k
        live2 = LiveForecaster(model, market.with_id_prices(hidden), lo, hi, offset)
        assert np.allclose(forecast, live2.forecast(k)), k
        assert len(forecast) == hi - lo - k
    # visible information does move the forecast
    shock = market.id_price.copy()
    end = LiveForecaster(model, market, lo, hi, offset).visible_end(40)
    shock[end - 1] += 250.0
    moved = LiveForecaster(model, market.with_id_prices(shock), lo, hi, offset).forecast(40)
    assert moved[0] > LiveForecaster(model, market, lo, hi, offset).forecast(40)[0]


def test_settings_validation() -> None:
    with pytest.raises(ValueError):
        ForecastSettings(profile_days=1)
    with pytest.raises(ValueError):
        ForecastSettings(winsor_eur_mwh=0.0)
    with pytest.raises(ValueError):
        replace(ForecastSettings(), beta_min_t=-1.0)
