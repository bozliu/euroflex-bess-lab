from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from euroflex_bess_lab.realtime import PublicationModel
from euroflex_bess_lab.realtime.forecasting import (
    DiscreteForecast,
    GBMParams,
    PriceForecaster,
    PriceForecasterConfig,
    PrismForecaster,
    PrismForecasterConfig,
    crps_discrete,
    forecast_skill,
    monthly_folds,
    quantile_weights,
    single_fold,
    slot_climatology,
    tail_mean,
    training_rows,
    weighted_quantile,
    window_rows,
)
from euroflex_bess_lab.realtime.forecasting.state import regime_components
from euroflex_bess_lab.realtime.forecasting.synthetic import synthetic_market

FAST = GBMParams(max_iter=15, max_leaf_nodes=15, min_samples_leaf=20)


def test_monthly_folds_train_strictly_before_test() -> None:
    folds = monthly_folds("2024-06-01", "2024-10-15", data_start="2024-05-22", train_months=2)
    assert [f.label for f in folds] == ["2024-06", "2024-07", "2024-08", "2024-09", "2024-10"]
    for fold in folds:
        assert fold.train_end == fold.test_start
        assert fold.train_start >= pd.Timestamp("2024-05-22", tz="UTC")
        assert fold.test_start - fold.train_start <= pd.Timedelta(days=62)
    assert folds[-1].test_end == pd.Timestamp("2024-10-15", tz="UTC")
    expanding = monthly_folds("2024-06-01", "2024-09-01", data_start="2024-05-22", train_months=None)
    assert all(f.train_start == pd.Timestamp("2024-05-22", tz="UTC") for f in expanding)
    # too little history: the month is skipped rather than trained on nothing
    assert monthly_folds("2024-06-01", "2024-07-01", data_start="2024-05-28", min_train_days=7) == []
    with pytest.raises(ValueError):
        single_fold("2024-06-02", "2024-06-01", "2024-06-03", "2024-06-04")


@pytest.mark.parametrize("horizon", [0, 1])
def test_training_rows_never_touch_the_test_window(horizon: int) -> None:
    market = synthetic_market(days=10, seed=1)
    publication = PublicationModel(qh_final_lag_minutes=5)
    start = market.start_utc
    fold = single_fold(start, start + pd.Timedelta(days=6), start + pd.Timedelta(days=6), market.end_utc)[0]
    rows = training_rows(market, fold, horizon=horizon, publication=publication, stride=2)
    test = window_rows(market, fold)
    fit_minute = 6 * 1440
    assert rows.max() < test.min() == fit_minute
    # embargo: every training target was published by the fit time
    target_qh = rows // 15 + horizon
    published = (target_qh + 1) * 15 + publication.qh_final_lag_minutes
    assert (published <= fit_minute).all()
    # ... and the embargo is tight: the last usable row is within two quarter-hours of the cut
    assert fit_minute - rows.max() <= 15 * (horizon + 2) + publication.qh_final_lag_minutes


def test_price_forecaster_predicts_only_its_test_windows() -> None:
    market = synthetic_market(days=12, seed=3)
    start = market.start_utc
    folds = single_fold(start, start + pd.Timedelta(days=8), start + pd.Timedelta(days=8), market.end_utc)
    forecaster = PriceForecaster(PriceForecasterConfig(params=FAST, stride=4)).fit(market, folds)
    frame = forecaster.predict(market)
    cut = 8 * 1440
    assert np.isnan(frame.current.mean[:cut]).all()
    assert np.isfinite(frame.current.mean[cut:]).all()
    assert np.isfinite(frame.next.values[cut:-15]).all()
    assert np.all(np.diff(frame.current.values[cut:], axis=1) >= 0)  # sorted quantiles
    assert np.allclose(frame.current.weights[cut:].sum(axis=1), 1.0)
    p = frame.current.p_short[cut:]
    assert np.all((p >= 0) & (p <= 1))
    # the forecast beats persistence of the last published price on the synthetic market
    skill = forecast_skill(
        frame.current,
        market,
        market.settlement("current"),
        si_qh=market.qh["si_mw"],
        rows=np.arange(cut, market.n_minutes),
        running_field="price_cum_eur_mwh",
    )
    assert skill.loc["all", "mae_mean"] < skill.loc["all", "mae_persistence"]
    assert skill.loc["all", "brier"] < skill.loc["all", "brier_climatology"]
    assert skill.loc["14", "pinball"] < skill.loc["0", "pinball"]
    assert list(skill.index[:3]) == ["0", "1", "2"]


def test_prism_mixture_is_consistent_for_both_mechanisms() -> None:
    market = synthetic_market(days=10, seed=5)
    start = market.start_utc
    folds = single_fold(start, start + pd.Timedelta(days=7), start + pd.Timedelta(days=7), market.end_utc)
    for mechanism, components in (("current", "published"), ("new2026", "reconstructed"), ("new2026", "published")):
        config = PrismForecasterConfig(mechanism=mechanism, components=components, params=FAST, stride=4)
        frame = PrismForecaster(config).fit(market, folds).predict(market)
        rows = np.arange(7 * 1440, market.n_minutes - 15)
        cur = frame.current
        assert cur.n_points == 10
        assert np.allclose(cur.weights[rows].sum(axis=1), 1.0, atol=1e-5)
        p = cur.p_short[rows]
        assert np.allclose(cur.weights[rows, :5].sum(axis=1), p, atol=1e-5)
        assert np.isfinite(cur.mean[rows]).all()
        up, down = regime_components(market, mechanism, components)
        assert np.isfinite(up).mean() > 0.9 and np.isfinite(down).mean() > 0.9


def test_zero_history_prism_never_reads_new_formula_prices() -> None:
    """The zero-history forecaster must give identical forecasts whether or not new-formula tables are loaded."""
    with_nf = synthetic_market(days=9, seed=8, include_new_formula=True)
    without = synthetic_market(days=9, seed=8, include_new_formula=False)
    start = with_nf.start_utc
    folds = single_fold(start, start + pd.Timedelta(days=6), start + pd.Timedelta(days=6), with_nf.end_utc)
    config = PrismForecasterConfig(mechanism="new2026", components="reconstructed", params=FAST, stride=4)
    a = PrismForecaster(config).fit(with_nf, folds).predict(with_nf)
    b = PrismForecaster(config).fit(without, folds).predict(without)
    assert np.allclose(a.current.mean, b.current.mean, equal_nan=True)
    assert np.allclose(a.history_prices, b.history_prices, equal_nan=True)


def test_distribution_helpers() -> None:
    values = np.array([[0.0, 10.0, 20.0, 30.0]])
    weights = np.array([[0.25, 0.25, 0.25, 0.25]])
    assert weighted_quantile(values, weights, 0.5)[0] == 10.0
    assert weighted_quantile(values, weights, 0.51)[0] == 20.0
    assert tail_mean(values, weights, 0.5, lower=True)[0] == pytest.approx(5.0)
    assert tail_mean(values, weights, 0.25, lower=False)[0] == pytest.approx(30.0)
    assert crps_discrete(np.array([[5.0]]), np.array([[1.0]]), np.array([7.0]))[0] == pytest.approx(2.0)
    assert np.allclose(quantile_weights((0.1, 0.5, 0.9)), [0.3, 0.4, 0.3])
    empty = DiscreteForecast.empty(3, 4)
    assert not empty.available().any()


def test_slot_climatology_uses_only_previous_days() -> None:
    prices = np.arange(96 * 5, dtype=float)
    values, weights = slot_climatology(prices, days=2, neighbours=0, levels=(0.25, 0.75), min_samples=2)
    assert np.isnan(values[: 96 * 2]).all()
    j = 96 * 3 + 7
    assert values[j].tolist() == pytest.approx([j - 192 + 24.0, j - 96 - 24.0])
    assert weights.sum() == pytest.approx(1.0)
    # the row for j never depends on j itself or anything after it
    shifted = prices.copy()
    shifted[j:] = -1e6
    values2, _ = slot_climatology(shifted, days=2, neighbours=0, levels=(0.25, 0.75), min_samples=2)
    assert np.allclose(values[: j + 1], values2[: j + 1], equal_nan=True)


def test_merging_fold_fits_equals_one_walk_forward_fit() -> None:
    from euroflex_bess_lab.realtime.forecasting import merge_forecasters

    market = synthetic_market(days=12, seed=13)
    start = market.start_utc
    day = pd.Timedelta(days=1)
    folds = [
        single_fold(start, start + 6 * day, start + 6 * day, start + 9 * day)[0],
        single_fold(start + 3 * day, start + 9 * day, start + 9 * day, market.end_utc)[0],
    ]
    config = PriceForecasterConfig(params=FAST, stride=4)
    whole = PriceForecaster(config).fit(market, folds).predict(market)
    parts = [PriceForecaster(config).fit(market, [fold]) for fold in reversed(folds)]
    merged = merge_forecasters(parts)
    assert [f.label for f in merged.folds_] == [f.label for f in folds]
    again = merged.predict(market)
    assert np.allclose(whole.current.mean, again.current.mean, equal_nan=True)
    with pytest.raises(ValueError):
        merge_forecasters(
            [PriceForecaster(config).fit(market, [folds[0]]), PriceForecaster(config).fit(market, [folds[0]])]
        )


def test_direct_model_on_rebuilt_targets_is_zero_history() -> None:
    """The ablation arm learns operator-rebuilt new-formula prices and never reads a new-formula price."""
    from euroflex_bess_lab.realtime.forecasting import INVARIANT_GROUPS, REBUILT_PRICE_BASELINE, FeatureConfig
    from euroflex_bess_lab.realtime.mechanisms import reconstruct_new_formula_history

    with_nf = synthetic_market(days=9, seed=19, include_new_formula=True)
    without = synthetic_market(days=9, seed=19, include_new_formula=False)
    start = with_nf.start_utc
    folds = single_fold(start, start + pd.Timedelta(days=6), start + pd.Timedelta(days=6), with_nf.end_utc)
    config = PriceForecasterConfig(
        mechanism="new2026",
        features=FeatureConfig(groups=INVARIANT_GROUPS, history="reconstructed:new2026"),
        baseline=REBUILT_PRICE_BASELINE,
        target="reconstructed:new2026",
        params=FAST,
        stride=4,
    )
    a = PriceForecaster(config).fit(with_nf, folds)
    rebuilt = reconstruct_new_formula_history(pd.DataFrame(with_nf.qh))["imbalance_price_eur_mwh"].to_numpy()
    assert np.allclose(a.history_prices(with_nf), rebuilt, equal_nan=True)
    assert not np.allclose(a.history_prices(with_nf), with_nf.settlement("new2026"), equal_nan=True)
    fa = a.predict(with_nf)
    fb = PriceForecaster(config).fit(without, folds).predict(without)
    assert np.allclose(fa.current.mean, fb.current.mean, equal_nan=True)
    assert np.isfinite(fa.current.mean[6 * 1440 :]).all()
