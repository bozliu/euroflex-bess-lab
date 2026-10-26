"""Look-ahead audits of the forecast strategies on real Elia data.

The fast tests use the committed fixture slices (``tests/fixtures/public``); the slow test uses the full
``data/public`` tree (``EUROFLEX_RUN_SLOW=1``).
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from euroflex_bess_lab.realtime import BatteryParams, MarketData, check_no_lookahead, simulate
from euroflex_bess_lab.realtime.forecasting import (
    GBMParams,
    PriceForecaster,
    PriceForecasterConfig,
    PrismForecaster,
    PrismForecasterConfig,
    monthly_folds,
    single_fold,
)
from euroflex_bess_lab.realtime.strategies.forecast_mpc import ForecastMPCStrategy, MPCConfig
from euroflex_bess_lab.realtime.strategies.prism import PrismStrategy

FIXTURES = Path(__file__).resolve().parent / "fixtures" / "public"
FAST = GBMParams(max_iter=12, max_leaf_nodes=15, min_samples_leaf=20)
BATTERY = BatteryParams(power_mw=1.0, energy_mwh=2.0)


@pytest.fixture
def fixture_market(monkeypatch: pytest.MonkeyPatch) -> MarketData:
    if not (FIXTURES / "belgium" / "minute_2026.parquet").exists():
        pytest.skip("public-data fixtures not available")
    monkeypatch.setenv("EUROFLEX_DATA_DIR", str(FIXTURES))
    return MarketData.from_public("2026-03-02T00:00:00Z", "2026-03-05T00:00:00Z")


def test_forecast_strategies_pass_audit_on_real_fixture_days(fixture_market: MarketData) -> None:
    market = fixture_market
    assert {"current", "new2026"} <= set(market.settlement_prices)
    start = market.start_utc
    folds = single_fold(start, start + pd.Timedelta(days=1), start + pd.Timedelta(days=1), market.end_utc)
    price = PriceForecaster(PriceForecasterConfig(params=FAST, stride=2)).fit(market, folds)
    prism = PrismForecaster(PrismForecasterConfig(params=FAST, stride=2)).fit(market, folds)
    zero = PrismForecaster(
        PrismForecasterConfig(mechanism="new2026", components="reconstructed", params=FAST, stride=2)
    ).fit(market, folds)
    test = market.slice(start + pd.Timedelta(days=1), market.end_utc)
    cases = [
        (lambda: ForecastMPCStrategy(price, MPCConfig(risk="quantile")), "current"),
        (lambda: PrismStrategy(prism), "current"),
        (lambda: PrismStrategy(zero), "new2026"),
        (lambda: ForecastMPCStrategy(price, name="naive"), "new2026"),
    ]
    for make, mechanism in cases:
        for cut in (31, 1500):
            audit = check_no_lookahead(make, test, BATTERY, cut_minute=cut, mechanism=mechanism)
            assert audit.passed, (mechanism, audit)
    result = simulate(test, PrismStrategy(prism), BATTERY, record_minutes=False)
    assert np.isfinite(result.pnl_eur)


@pytest.mark.slow
def test_walk_forward_month_on_full_data_is_causal_and_profitable() -> None:
    market = MarketData.from_public("2025-05-01T00:00:00Z", "2025-07-01T00:00:00Z")
    folds = monthly_folds("2025-06-01", "2025-07-01", data_start="2025-05-01", train_months=1)
    forecaster = PriceForecaster(PriceForecasterConfig(params=GBMParams(max_iter=60), stride=4)).fit(market, folds)
    june = market.slice("2025-06-01T00:00:00Z", "2025-07-01T00:00:00Z")
    result = simulate(june, ForecastMPCStrategy(forecaster, MPCConfig(risk="quantile")), BATTERY, record_minutes=False)
    assert result.pnl_eur > 0.0
    window = june.slice("2025-06-10T00:00:00Z", "2025-06-13T00:00:00Z")
    for cut in (37, 1503, 2999):
        audit = check_no_lookahead(lambda: ForecastMPCStrategy(forecaster), window, BATTERY, cut_minute=cut)
        assert audit.passed, audit
