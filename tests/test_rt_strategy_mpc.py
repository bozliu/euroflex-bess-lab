from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from euroflex_bess_lab.realtime import (
    BatteryParams,
    IdleStrategy,
    MarketData,
    OracleQHStrategy,
    PublicationModel,
    ThresholdStrategy,
    check_no_lookahead,
    simulate,
)
from euroflex_bess_lab.realtime.forecasting import GBMParams, PriceForecaster, PriceForecasterConfig, single_fold
from euroflex_bess_lab.realtime.forecasting.synthetic import synthetic_market
from euroflex_bess_lab.realtime.strategies.forecast_mpc import (
    CachedForecaster,
    ForecastMPCStrategy,
    MPCConfig,
    SocGrid,
    bellman_stage,
    remaining_qh_energy,
)

FAST = GBMParams(max_iter=25, max_leaf_nodes=15, min_samples_leaf=20)
BATTERY = BatteryParams(power_mw=1.0, energy_mwh=2.0)


@pytest.fixture(scope="module")
def fitted() -> tuple[MarketData, CachedForecaster]:
    market = synthetic_market(days=14, seed=21)
    start = market.start_utc
    folds = single_fold(start, start + pd.Timedelta(days=9), start + pd.Timedelta(days=9), market.end_utc)
    forecaster = PriceForecaster(PriceForecasterConfig(params=FAST, stride=4)).fit(market, folds)
    test = market.slice(start + pd.Timedelta(days=9), market.end_utc)
    return test, CachedForecaster(forecaster)


def test_soc_grid_bands_cover_full_power() -> None:
    grid = SocGrid.build(BATTERY, steps_per_qh=6)
    assert grid.levels[0] == pytest.approx(BATTERY.soc_min_mwh)
    assert grid.levels[-1] == pytest.approx(BATTERY.soc_max_mwh)
    energies = [band.energy_mwh for band in grid.bands]
    assert max(energies) == pytest.approx(0.25)
    assert min(energies) == pytest.approx(-0.25)
    assert 0.0 in energies


def test_bellman_stage_values_stored_energy() -> None:
    grid = SocGrid.build(BATTERY, steps_per_qh=6)
    n = grid.n
    zero = np.zeros((1, n), dtype=np.float32)
    # one certain high price: value grows with SOC, up to one quarter-hour of full discharge
    v = bellman_stage(zero, np.array([[200.0]], dtype=np.float32), np.array([[1.0]], dtype=np.float32), grid, 8.0)
    assert v[0, 0] == pytest.approx(0.0)
    assert np.all(np.diff(v[0]) >= -1e-4)
    assert v[0, -1] == pytest.approx(0.25 * (200.0 - 8.0), rel=1e-4)
    # a certain negative price is worth charging into an empty battery
    v_neg = bellman_stage(zero, np.array([[-100.0]], dtype=np.float32), np.array([[1.0]], dtype=np.float32), grid, 8.0)
    assert v_neg[0, 0] == pytest.approx(25.0, rel=1e-4)
    # rows without prices keep the continuation value
    v_nan = bellman_stage(
        zero + 3.0, np.array([[np.nan]], dtype=np.float32), np.array([[1.0]], dtype=np.float32), grid, 8.0
    )
    assert np.allclose(v_nan, 3.0)


def test_remaining_energy_follows_water_value() -> None:
    grid = SocGrid.build(BATTERY, steps_per_qh=6)
    slopes = np.full(grid.n - 1, 60.0, dtype=np.float32)  # stored energy is worth 60 EUR/MWh
    soc = BATTERY.initial_soc_mwh
    high = remaining_qh_energy(soc, slopes, grid, BATTERY, 150.0, 150.0, minutes_left=15)
    assert high == pytest.approx(0.25)
    low = remaining_qh_energy(soc, slopes, grid, BATTERY, 20.0, 20.0, minutes_left=6)
    assert low == pytest.approx(-0.1)
    hold = remaining_qh_energy(soc, slopes, grid, BATTERY, 62.0, 62.0, minutes_left=15)
    assert hold == 0.0
    # risk prices: a low discharge quantile blocks the discharge
    assert remaining_qh_energy(soc, slopes, grid, BATTERY, 50.0, 150.0, minutes_left=15) == 0.0
    # cannot discharge an empty battery
    assert remaining_qh_energy(BATTERY.soc_min_mwh, slopes, grid, BATTERY, 500.0, 500.0, minutes_left=15) == 0.0


def test_no_discharge_at_negative_prices_by_default() -> None:
    """At the -15,000 EUR/MWh floor, "discharge now to make room" looks profitable against a value function that
    expects one more floor-price quarter-hour; by default the strategy never pays to inject."""
    grid = SocGrid.build(BATTERY, steps_per_qh=6)
    slopes = np.full(grid.n - 1, -15_700.0, dtype=np.float32)  # stored energy has negative value (room is worth more)
    soc = 1.8
    blocked = remaining_qh_energy(soc, slopes, grid, BATTERY, -14_950.0, -14_950.0, minutes_left=15)
    allowed = remaining_qh_energy(
        soc, slopes, grid, BATTERY, -14_950.0, -14_950.0, minutes_left=15, allow_negative_price_discharge=True
    )
    assert allowed > 0.0  # the first leg of the round trip
    assert blocked < 0.0  # instead it charges the remaining room and is paid for it


def test_discount_is_a_scaling_of_the_continuation_value() -> None:
    grid = SocGrid.build(BATTERY, steps_per_qh=6)
    rng = np.random.default_rng(0)
    v_next = np.sort(rng.normal(0.0, 50.0, size=(3, grid.n)).astype(np.float32), axis=1)
    prices = rng.normal(80.0, 60.0, size=(3, 4)).astype(np.float32)
    probs = np.full((3, 4), 0.25, dtype=np.float32)
    a = bellman_stage(v_next, prices, probs, grid, 8.0, discount=0.9)
    b = bellman_stage((0.9 * v_next).astype(np.float32), prices, probs, grid, 8.0)
    assert np.allclose(a, b, atol=1e-4)
    with pytest.raises(ValueError):
        MPCConfig(discount_per_qh=0.0)


@pytest.mark.parametrize("risk", ["neutral", "quantile", "cvar"])
def test_forecast_mpc_earns_and_stays_below_oracle(fitted: tuple[MarketData, CachedForecaster], risk: str) -> None:
    market, forecaster = fitted
    result = simulate(market, ForecastMPCStrategy(forecaster, MPCConfig(risk=risk)), BATTERY, record_minutes=False)
    oracle = simulate(market, OracleQHStrategy(), BATTERY, record_minutes=False)
    idle = simulate(market, IdleStrategy(), BATTERY, record_minutes=False)
    assert idle.pnl_eur == 0.0
    assert 0.0 < result.pnl_eur < oracle.pnl_eur
    summary = result.summary(oracle)
    assert 0.0 < summary["capture_vs_oracle"] < 1.0
    assert summary["cycles_per_day"] > 0.1


def test_forecast_mpc_beats_reactive_threshold_on_synthetic(fitted: tuple[MarketData, CachedForecaster]) -> None:
    market, forecaster = fitted
    mpc = simulate(market, ForecastMPCStrategy(forecaster), BATTERY, record_minutes=False)
    rule = simulate(market, ThresholdStrategy(min_history_qh=10_000), BATTERY, record_minutes=False)
    assert mpc.pnl_eur > rule.pnl_eur


@pytest.mark.parametrize(("lag", "cut"), [(1, 1503), (0, 2210), (3, 2701)])
def test_forecast_mpc_passes_lookahead_audit(fitted: tuple[MarketData, CachedForecaster], lag: int, cut: int) -> None:
    market, forecaster = fitted
    window = market.slice(market.start_utc, market.start_utc + pd.Timedelta(days=2))
    publication = PublicationModel(minute_lag=lag, qh_final_lag_minutes=5)
    audit = check_no_lookahead(
        lambda: ForecastMPCStrategy(forecaster.forecaster, MPCConfig(risk="cvar")),
        window,
        BATTERY,
        cut_minute=cut,
        publication=publication,
    )
    assert audit.passed, audit


def test_forecast_mpc_without_forecasts_idles(fitted: tuple[MarketData, CachedForecaster]) -> None:
    market, forecaster = fitted
    before = market.slice(market.start_utc - pd.Timedelta(days=0), market.start_utc + pd.Timedelta(days=1))
    # a market entirely outside the fitted test window gets no forecast and therefore no trades
    shifted = MarketData(
        **{
            **before.__dict__,
            "start_utc": before.start_utc - pd.Timedelta(days=30),
            "end_utc": before.end_utc - pd.Timedelta(days=30),
            "minute_times": before.minute_times - pd.Timedelta(days=30),
            "qh_times": before.qh_times - pd.Timedelta(days=30),
        }
    )
    result = simulate(shifted, ForecastMPCStrategy(forecaster.forecaster), BATTERY, record_minutes=False)
    assert result.pnl_eur == 0.0


def test_config_validation() -> None:
    with pytest.raises(ValueError):
        MPCConfig(horizon_qh=0)
    with pytest.raises(ValueError):
        MPCConfig(replan_minutes=(15,))
    with pytest.raises(ValueError):
        MPCConfig(risk_quantile=0.6)
