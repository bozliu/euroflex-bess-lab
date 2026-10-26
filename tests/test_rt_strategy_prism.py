from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from euroflex_bess_lab.realtime import BatteryParams, MarketData, PublicationModel, check_no_lookahead, simulate
from euroflex_bess_lab.realtime.forecasting import (
    GBMParams,
    PriceForecaster,
    PriceForecasterConfig,
    PrismForecaster,
    PrismForecasterConfig,
    single_fold,
)
from euroflex_bess_lab.realtime.forecasting.synthetic import synthetic_market
from euroflex_bess_lab.realtime.strategies.forecast_mpc import CachedForecaster, ForecastMPCStrategy, MPCConfig, MPCPlan
from euroflex_bess_lab.realtime.strategies.prism import PrismStrategy, fit_prism

FAST = GBMParams(max_iter=25, max_leaf_nodes=15, min_samples_leaf=20)
BATTERY = BatteryParams(power_mw=1.0, energy_mwh=2.0)


@pytest.fixture(scope="module")
def setup() -> tuple[MarketData, PrismForecaster, PrismForecaster]:
    market = synthetic_market(days=12, seed=31)
    start = market.start_utc
    folds = single_fold(start, start + pd.Timedelta(days=8), start + pd.Timedelta(days=8), market.end_utc)
    current = fit_prism(market, folds, mechanism="current", params=FAST, stride=4)
    zero = fit_prism(market, folds, mechanism="new2026", components="reconstructed", params=FAST, stride=4)
    return market.slice(start + pd.Timedelta(days=8), market.end_utc), current, zero


def _perturbed(market: MarketData, cut: int, seed: int = 5) -> MarketData:
    rng = np.random.default_rng(seed)
    q_cut = cut // 15

    def noise(values: np.ndarray, start: int) -> np.ndarray:
        out = values.copy()
        out[start:] = rng.normal(0.0, 500.0, size=len(out) - start)
        return out

    hidden = market.da_visible_from_minute >= cut
    da = market.da_price.copy()
    da[hidden] = rng.normal(0.0, 500.0, size=int(hidden.sum()))
    return MarketData(
        **{
            **market.__dict__,
            "minute": {k: noise(v, cut) for k, v in market.minute.items()},
            "qh": {k: noise(v, q_cut) for k, v in market.qh.items()},
            "settlement_prices": {k: noise(v, q_cut) for k, v in market.settlement_prices.items()},
            "da_price": da,
        }
    )


@pytest.mark.parametrize("cut", [40, 1503, 2702])
def test_white_box_plan_is_causal(setup: tuple[MarketData, PrismForecaster, PrismForecaster], cut: int) -> None:
    """Every precomputed plan array (risk prices, value-function slopes) for rows before the safe horizon must be
    identical when all unpublished data is replaced by noise - a stronger check than comparing decisions."""
    market, current, zero = setup
    publication = PublicationModel(minute_lag=1, qh_final_lag_minutes=5)
    safe_until = min(cut + 2, (cut // 15) * 15 + 20)
    for forecaster, mechanism in ((current, "current"), (zero, "new2026")):
        config = MPCConfig(risk="quantile")
        a = MPCPlan.build(forecaster.predict(market, publication), market, BATTERY, publication, config)
        perturbed = _perturbed(market, cut)
        b = MPCPlan.build(forecaster.predict(perturbed, publication), perturbed, BATTERY, publication, config)
        assert np.allclose(a.price_discharge[:safe_until], b.price_discharge[:safe_until], equal_nan=True)
        assert np.allclose(a.price_charge[:safe_until], b.price_charge[:safe_until], equal_nan=True)
        assert np.array_equal(a.plan_row[:safe_until], b.plan_row[:safe_until])
        used = np.unique(a.plan_row[:safe_until][a.plan_row[:safe_until] >= 0])
        assert np.allclose(a.slopes[used], b.slopes[used], equal_nan=True), mechanism


def test_prism_strategies_pass_the_audit(setup: tuple[MarketData, PrismForecaster, PrismForecaster]) -> None:
    market, current, zero = setup
    window = market.slice(market.start_utc, market.start_utc + pd.Timedelta(days=2))
    for forecaster, mechanism in ((current, "current"), (zero, "new2026")):
        for cut in (31, 1499, 2190):
            audit = check_no_lookahead(
                lambda f=forecaster: PrismStrategy(f), window, BATTERY, cut_minute=cut, mechanism=mechanism
            )
            assert audit.passed, (mechanism, audit)


def test_prism_trades_profitably_and_checks_mechanism(
    setup: tuple[MarketData, PrismForecaster, PrismForecaster],
) -> None:
    market, current, zero = setup
    res_cur = simulate(market, PrismStrategy(CachedForecaster(current)), BATTERY, record_minutes=False)
    res_new = simulate(market, PrismStrategy(zero), BATTERY, mechanism="new2026", record_minutes=False)
    assert res_cur.pnl_eur > 0.0 and res_new.pnl_eur > 0.0
    assert res_new.strategy == "prism_new2026"
    with pytest.raises(ValueError, match="operator"):
        simulate(market, PrismStrategy(zero), BATTERY, mechanism="current", record_minutes=False)
    with pytest.raises(TypeError):
        PrismStrategy(PriceForecaster(PriceForecasterConfig(params=FAST)))  # type: ignore[arg-type]


def test_naive_transfer_baseline_runs_under_the_new_mechanism(
    setup: tuple[MarketData, PrismForecaster, PrismForecaster],
) -> None:
    """A current-formula price forecaster may be applied naively to new-formula settlement (experiment arm a)."""
    market, _, _ = setup
    full = synthetic_market(days=12, seed=31)
    start = full.start_utc
    folds = single_fold(start, start + pd.Timedelta(days=8), start + pd.Timedelta(days=8), full.end_utc)
    naive = PriceForecaster(PriceForecasterConfig(params=FAST, stride=4)).fit(full, folds)
    result = simulate(
        market, ForecastMPCStrategy(naive, name="naive"), BATTERY, mechanism="new2026", record_minutes=False
    )
    assert np.isfinite(result.pnl_eur)
    assert isinstance(PrismForecasterConfig().feature_config().groups, tuple)
