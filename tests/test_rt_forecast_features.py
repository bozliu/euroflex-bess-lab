from __future__ import annotations

import numpy as np
import pytest

from euroflex_bess_lab.realtime import MarketData, PublicationModel
from euroflex_bess_lab.realtime.forecasting.features import (
    ALL_GROUPS,
    INVARIANT_GROUPS,
    FeatureConfig,
    baseline_values,
    build_features,
    minute_target,
    rolling_qh_stat,
    target_known_minute,
)
from euroflex_bess_lab.realtime.forecasting.synthetic import synthetic_market


def _perturb_future(market: MarketData, cut: int, seed: int = 11) -> MarketData:
    """Replace everything that is not yet published at ``cut`` with noise (same rule as the look-ahead audit)."""
    rng = np.random.default_rng(seed)
    q_cut = cut // 15
    minute = {k: v.copy() for k, v in market.minute.items()}
    for v in minute.values():
        v[cut:] = rng.normal(0.0, 500.0, size=len(v) - cut)
    qh = {k: v.copy() for k, v in market.qh.items()}
    for v in qh.values():
        v[q_cut:] = rng.normal(0.0, 500.0, size=len(v) - q_cut)
    settlement = {k: v.copy() for k, v in market.settlement_prices.items()}
    for v in settlement.values():
        v[q_cut:] = rng.normal(0.0, 500.0, size=len(v) - q_cut)
    da = market.da_price.copy()
    hidden = market.da_visible_from_minute >= cut
    da[hidden] = rng.normal(0.0, 500.0, size=int(hidden.sum()))
    return MarketData(
        **{**market.__dict__, "minute": minute, "qh": qh, "settlement_prices": settlement, "da_price": da}
    )


@pytest.mark.parametrize("lag", [0, 1, 3])
@pytest.mark.parametrize("cut", [1503, 2107, 2880])
def test_features_never_use_unpublished_rows(lag: int, cut: int) -> None:
    market = synthetic_market(days=3, seed=4)
    publication = PublicationModel(minute_lag=lag, qh_final_lag_minutes=5)
    config = FeatureConfig(groups=ALL_GROUPS, history="settlement:current")
    base = build_features(market, publication, config)
    other = build_features(_perturb_future(market, cut), publication, config)
    q_cut = cut // 15
    safe_until = min(cut + lag + 1, q_cut * 15 + 15 + publication.qh_final_lag_minutes)
    a, b = base.values[:safe_until], other.values[:safe_until]
    same = np.isclose(a, b, equal_nan=True)
    assert same.all(), [base.names[j] for j in np.flatnonzero(~same.all(axis=0))]
    # sanity: the perturbation is visible right after the safe horizon for minute-level features
    assert not np.isclose(base.values[safe_until + 20 :], other.values[safe_until + 20 :], equal_nan=True).all()


def test_reconstructed_history_is_causal() -> None:
    market = synthetic_market(days=3, seed=5)
    config = FeatureConfig(groups=INVARIANT_GROUPS, history="reconstructed:new2026")
    publication = PublicationModel()
    cut = 1600
    base = build_features(market, publication, config)
    other = build_features(_perturb_future(market, cut), publication, config)
    safe_until = min(cut + 2, (cut // 15) * 15 + 20)
    assert np.isclose(base.values[:safe_until], other.values[:safe_until], equal_nan=True).all()
    assert np.isfinite(base.column("hist_price_last")[200:]).all()


def test_feature_names_are_stable_when_fields_are_missing() -> None:
    full = synthetic_market(days=2, seed=1)
    thin = synthetic_market(days=2, seed=1, include_new_formula=False, include_si_forecast=False)
    config = FeatureConfig(groups=ALL_GROUPS)
    a = build_features(full, PublicationModel(), config)
    b = build_features(thin, PublicationModel(), config)
    assert a.names == b.names
    assert np.isnan(b.column("nf_price_cum")).all()
    assert np.isnan(b.column("elia_si_fc")).all()
    assert a.values.dtype == np.float32 and a.values.shape == (full.n_minutes, len(a.names))


def test_running_values_respect_the_quarter_hour_and_lag() -> None:
    market = synthetic_market(days=1, seed=2)
    publication = PublicationModel(minute_lag=1)
    feats = build_features(market, publication, FeatureConfig(groups=("calendar", "si", "current_mech")))
    t = np.arange(market.n_minutes)
    m = t % 15
    si_cum = feats.column("si_cum")
    # no visible row of the current quarter-hour at minutes 0 and 1 (row t - 2 belongs to the previous one)
    assert np.isnan(si_cum[m <= 1]).all()
    rows = np.flatnonzero(m >= 2)
    expected = market.minute["si_cum_mw"][rows - 2]
    assert np.allclose(si_cum[rows], expected.astype(np.float32))
    assert np.allclose(feats.column("n_visible_minutes"), np.clip(m - 1, 0, 15))
    # the previous quarter-hour value is its last row once visible
    prev = feats.column("cur_price_prev_qh")
    rows = np.flatnonzero((m >= 2) & (t >= 15))
    assert np.allclose(prev[rows], market.minute["price_cum_eur_mwh"][(t[rows] // 15) * 15 - 1].astype(np.float32))


def test_history_features_wait_for_final_publication() -> None:
    market = synthetic_market(days=1, seed=3)
    publication = PublicationModel(qh_final_lag_minutes=5)
    feats = build_features(market, publication, FeatureConfig(groups=("history",)))
    last = feats.column("hist_price_last")
    prices = market.settlement("current")
    # QH 0 ends at minute 15 and is published at minute 20
    assert np.isnan(last[:20]).all()
    assert last[20] == pytest.approx(prices[0], rel=1e-6)
    assert last[34] == pytest.approx(prices[0], rel=1e-6)
    assert last[35] == pytest.approx(prices[1], rel=1e-6)


def test_targets_and_known_minutes() -> None:
    market = synthetic_market(days=1, seed=0)
    prices = market.settlement("current")
    y0 = minute_target(market, prices, 0)
    y1 = minute_target(market, prices, 1)
    assert y0[0] == prices[0] and y0[14] == prices[0] and y0[15] == prices[1]
    assert y1[0] == prices[1] and np.isnan(y1[-1])
    known = target_known_minute(market, 0, PublicationModel(qh_final_lag_minutes=5))
    assert known[0] == 20 and known[29] == 35


def test_rolling_stats_and_baseline_chain() -> None:
    values = np.array([1.0, np.nan, 3.0, 5.0])
    assert np.allclose(rolling_qh_stat(values, 2), [1.0, 1.0, 3.0, 4.0])
    assert rolling_qh_stat(values, 3, "std")[3] == pytest.approx(1.0)
    x = np.array([[np.nan, 2.0], [1.0, 5.0], [np.nan, np.nan]], dtype=np.float32)
    assert baseline_values(x, ["a", "b"], ("a", "b", "missing")).tolist() == [2.0, 1.0, 0.0]
