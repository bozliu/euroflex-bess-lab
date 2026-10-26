from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from euroflex_bess_lab.realtime import BatteryParams, MarketData, PublicationModel, check_no_lookahead
from euroflex_bess_lab.realtime.forecasting import GBMParams, PrismForecaster, PrismForecasterConfig, single_fold
from euroflex_bess_lab.realtime.forecasting.fusion import (
    RunningEstimateFusion,
    running_error_variance,
    running_estimate,
)
from euroflex_bess_lab.realtime.forecasting.synthetic import synthetic_market
from euroflex_bess_lab.realtime.strategies.prism import PrismStrategy

FAST = GBMParams(max_iter=15, max_leaf_nodes=15, min_samples_leaf=20)


@pytest.fixture(scope="module")
def setup() -> tuple[MarketData, PrismForecaster]:
    market = synthetic_market(days=10, seed=17)
    start = market.start_utc
    folds = single_fold(start, start + pd.Timedelta(days=7), start + pd.Timedelta(days=7), market.end_utc)
    config = PrismForecasterConfig(mechanism="new2026", components="reconstructed", params=FAST, stride=4)
    return market, PrismForecaster(config).fit(market, folds)


def test_running_error_variance_shrinks_through_the_quarter_hour(setup: tuple[MarketData, PrismForecaster]) -> None:
    market, _ = setup
    start = market.start_utc
    variance = running_error_variance(
        market,
        "price_cum_eur_mwh",
        market.settlement("current"),
        PublicationModel(),
        start,
        start + pd.Timedelta(days=7),
    )
    assert np.isnan(variance[0]) and np.isnan(variance[14:]).all()  # lag 1: at most 13 visible minutes
    assert variance[13] < variance[1]
    running, visible = running_estimate(market, "price_cum_eur_mwh", PublicationModel())
    assert np.isnan(running[visible == 0]).all()


def test_fusion_moves_between_forecast_and_running_value(setup: tuple[MarketData, PrismForecaster]) -> None:
    market, base = setup
    rows = np.arange(7 * 1440, 10 * 1440)
    base_frame = base.predict(market)
    tight = RunningEstimateFusion(base, "nf_imbalance_price_eur_mwh", np.full(16, 1e-6)).predict(market)
    loose = RunningEstimateFusion(base, "nf_imbalance_price_eur_mwh", np.full(16, 1e12)).predict(market)
    running, visible = running_estimate(market, "nf_imbalance_price_eur_mwh", PublicationModel())
    seen = rows[visible[rows] > 0]
    unseen = rows[visible[rows] == 0]
    assert np.allclose(tight.current.mean[seen], running[seen], atol=1e-3)
    assert np.allclose(loose.current.mean[seen], base_frame.current.mean[seen], atol=1e-3)
    assert np.allclose(tight.current.mean[unseen], base_frame.current.mean[unseen], equal_nan=True)
    # the next-quarter-hour forecast is untouched
    assert np.allclose(tight.next.mean, base_frame.next.mean, equal_nan=True)


def test_fused_prism_strategy_passes_audit(setup: tuple[MarketData, PrismForecaster]) -> None:
    market, base = setup
    fused = RunningEstimateFusion(base, "nf_imbalance_price_eur_mwh", np.linspace(5000.0, 100.0, 16))
    window = market.slice(market.start_utc + pd.Timedelta(days=7), market.start_utc + pd.Timedelta(days=9))
    battery = BatteryParams(power_mw=1.0, energy_mwh=2.0)
    for cut in (40, 1503):
        audit = check_no_lookahead(lambda: PrismStrategy(fused), window, battery, cut_minute=cut, mechanism="new2026")
        assert audit.passed, audit
