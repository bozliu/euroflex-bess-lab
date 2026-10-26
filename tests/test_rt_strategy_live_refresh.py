"""Live re-planning: a strategy fed a window that fills minute by minute must decide exactly as in the backtest."""

from __future__ import annotations

from collections.abc import Callable

import numpy as np
import pandas as pd
import pytest

from euroflex_bess_lab.realtime import BatteryParams, DecisionContext, MarketData, PublicationModel, simulate
from euroflex_bess_lab.realtime.battery import apply_power
from euroflex_bess_lab.realtime.forecasting import (
    GBMParams,
    PriceForecaster,
    PriceForecasterConfig,
    PrismForecaster,
    PrismForecasterConfig,
    single_fold,
)
from euroflex_bess_lab.realtime.forecasting.synthetic import synthetic_market
from euroflex_bess_lab.realtime.strategies.base import Strategy
from euroflex_bess_lab.realtime.strategies.forecast_mpc import ForecastMPCStrategy, MPCConfig
from euroflex_bess_lab.realtime.strategies.prism import PrismStrategy

FAST = GBMParams(max_iter=12, max_leaf_nodes=15, min_samples_leaf=20)
BATTERY = BatteryParams(power_mw=1.0, energy_mwh=2.0)


def _empty_window(market: MarketData) -> MarketData:
    """Same grid, nothing published yet (day-ahead prices keep their own publication rule)."""

    def blank(values: dict[str, np.ndarray]) -> dict[str, np.ndarray]:
        return {k: np.full_like(v, np.nan) for k, v in values.items()}

    return MarketData(
        **{
            **market.__dict__,
            "minute": blank(market.minute),
            "qh": blank(market.qh),
            "settlement_prices": blank(market.settlement_prices),
        }
    )


def _publish(window: MarketData, full: MarketData, t: int, publication: PublicationModel) -> None:
    """Copy into ``window`` (in place) every row of ``full`` that is published by decision minute ``t``."""
    m_end = publication.visible_minute_end(t)
    q_end = publication.visible_qh_end(t)
    for name, values in full.minute.items():
        window.minute[name][:m_end] = values[:m_end]
    for name, values in full.qh.items():
        window.qh[name][:q_end] = values[:q_end]
    for name, values in full.settlement_prices.items():
        window.settlement_prices[name][:q_end] = values[:q_end]


def live_replay(
    make: Callable[[], Strategy],
    market: MarketData,
    *,
    start: int,
    minutes: int,
    mechanism: str = "current",
    publication: PublicationModel | None = None,
) -> tuple[np.ndarray, np.ndarray]:
    publication = publication or PublicationModel()
    backtest = simulate(market, make(), BATTERY, mechanism=mechanism, publication=publication)
    assert backtest.minute is not None
    expected = backtest.minute["power_mw"].to_numpy()
    soc_trace = backtest.minute["soc_mwh"].to_numpy()
    soc = BATTERY.initial_soc_mwh if start == 0 else float(soc_trace[start - 1])
    window = _empty_window(market)
    strategy = make()
    ctx = DecisionContext(window, BATTERY, publication, mechanism)
    got = np.zeros(minutes)
    last_power = 0.0 if start == 0 else float(expected[start - 1])
    for i, t in enumerate(range(start, start + minutes)):
        _publish(window, market, t, publication)
        ctx.market = MarketData(**window.__dict__)  # fresh view, arrays shared and filled in place
        ctx.advance(t, soc)
        ctx.last_power_mw = last_power
        if i == 0:
            strategy.reset(ctx)
        else:
            strategy.refresh(ctx)  # type: ignore[attr-defined]
        power, soc = apply_power(float(strategy.decide(ctx)), soc, BATTERY)
        got[i] = power
        last_power = power
    return got, expected[start : start + minutes]


@pytest.fixture(scope="module")
def fitted() -> tuple[MarketData, PriceForecaster, PrismForecaster]:
    full = synthetic_market(days=10, seed=23)
    start = full.start_utc
    folds = single_fold(start, start + pd.Timedelta(days=7), start + pd.Timedelta(days=7), full.end_utc)
    price = PriceForecaster(PriceForecasterConfig(params=FAST, stride=4)).fit(full, folds)
    zero = PrismForecaster(
        PrismForecasterConfig(mechanism="new2026", components="reconstructed", params=FAST, stride=4)
    ).fit(full, folds)
    test = full.slice(start + pd.Timedelta(days=7), start + pd.Timedelta(days=8))
    return test, price, zero


@pytest.mark.parametrize("start", [0, 700])
def test_forecast_mpc_live_refresh_matches_backtest(
    fitted: tuple[MarketData, PriceForecaster, PrismForecaster], start: int
) -> None:
    market, price, _ = fitted
    got, expected = live_replay(
        lambda: ForecastMPCStrategy(price, MPCConfig(risk="quantile")), market, start=start, minutes=50
    )
    assert np.abs(expected).max() > 0.0  # the window actually trades
    assert np.allclose(got, expected, atol=1e-9), np.flatnonzero(~np.isclose(got, expected, atol=1e-9))[:5]


def test_prism_live_refresh_matches_backtest(fitted: tuple[MarketData, PriceForecaster, PrismForecaster]) -> None:
    market, _, zero = fitted
    got, expected = live_replay(lambda: PrismStrategy(zero), market, start=300, minutes=40, mechanism="new2026")
    assert np.allclose(got, expected, atol=1e-9)


def test_rl_refresh_keeps_quarter_hour_state(fitted: tuple[MarketData, PriceForecaster, PrismForecaster]) -> None:
    pytest.importorskip("torch")
    from euroflex_bess_lab.realtime.strategies.rl import DQNConfig, RLStrategy, train_dqn

    market, _, _ = fitted
    full = synthetic_market(days=10, seed=23)
    start = full.start_utc
    config = DQNConfig(
        n_envs=16,
        batch_size=64,
        buffer_size=5_000,
        learning_starts=200,
        max_updates=150,
        validate_every=10_000,
        eps_decay_updates=100,
        max_minutes=0.5,
        device="cpu",
        hidden=(16,),
    )
    policy = train_dqn(full, BATTERY, train=(start, start + pd.Timedelta(days=6)), config=config)
    # start on a quarter-hour boundary: a cold reset() cannot know the energy already delivered mid-QH
    got, expected = live_replay(lambda: RLStrategy(policy), market, start=90, minutes=40)
    assert np.allclose(got, expected, atol=1e-9)
