from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from euroflex_bess_lab.realtime.impact import (
    AlphaModel,
    RegimeCurves,
    ResponseCurve,
    fit_alpha_model,
    fit_regime_curves,
    fit_response_curve,
    hour_groups,
    local_hour_regime,
)
from euroflex_bess_lab.realtime.impact.operators import estimate_pass_through, fit_current_response
from euroflex_bess_lab.realtime.impact.synthetic import synthetic_impact_market
from euroflex_bess_lab.realtime.impact.validation import (
    consecutive_pairs,
    error_stats,
    holdout_split,
    validate_curve_changes,
    validate_operator_si_shift,
)


def _panel(n_hours: int = 6000, slope: float = 0.15, seed: int = 0) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    rng = np.random.default_rng(seed)
    n = n_hours * 4
    groups = np.arange(n) // 4
    level = rng.normal(80.0, 25.0, n_hours)[groups]
    volume = np.clip(rng.normal(0.0, 200.0, n), 0.0, None) + rng.uniform(0.0, 30.0, n)
    noise = rng.standard_t(3, n) * 6.0
    price = level + slope * volume + noise
    price[rng.integers(0, n, 40)] += 4000.0  # scarcity spikes must not drive the fit
    return volume, price, groups


@pytest.mark.parametrize("estimator", ["first_difference", "within"])
def test_fixed_effect_fit_recovers_the_slope_despite_levels_and_spikes(estimator) -> None:
    volume, price, groups = _panel()
    curve = fit_response_curve(volume, price, increasing=True, groups=groups, estimator=estimator)
    knots = np.asarray(curve.knots)
    implied = np.asarray(curve.values)[1:] / knots[1:]
    # the first short segments are identified by small volume changes only (noisier); check the bulk of the curve
    assert np.all(np.abs(implied[knots[1:] >= 40.0] - 0.15) < 0.03)
    assert np.all(np.abs(implied - 0.15) < 0.06)
    assert curve.tail_slope == pytest.approx(0.15, abs=0.03)
    # without the fixed effect the level noise would dominate; with it the change prediction is calibrated
    assert curve.slope(np.array([100.0]))[0] == pytest.approx(0.15, abs=0.03)


def test_monotone_constraint_and_decreasing_curves() -> None:
    volume, price, groups = _panel(slope=-0.1)
    down = fit_response_curve(volume, price, increasing=False, groups=groups)
    assert np.all(np.diff(down.values) <= 1e-12)
    assert down.tail_slope <= 0.0
    # an increasing fit on a decreasing relation collapses to (near) flat instead of turning negative
    up = fit_response_curve(volume, price, increasing=True, groups=groups)
    assert np.all(np.diff(up.values) >= -1e-12)
    assert max(up.values) < 5.0


def test_unidentified_segments_stay_flat_and_curve_round_trips() -> None:
    rng = np.random.default_rng(3)
    volume = np.full(400, 50.0)
    price = rng.normal(100.0, 5.0, 400)
    curve = fit_response_curve(
        volume, price, increasing=True, groups=np.arange(400) // 4, knots=[0.0, 25.0, 50.0, 100.0]
    )
    assert np.allclose(curve.values, 0.0)
    again = ResponseCurve.from_dict(curve.to_dict())
    assert again == curve
    with pytest.raises(ValueError):
        ResponseCurve(knots=(0.0, 1.0), values=(0.0, -1.0), increasing=True, tail_slope=0.0)


def test_regime_curves_use_pooled_fallback() -> None:
    volume, price, groups = _panel(n_hours=2000)
    regime = np.where(np.arange(len(volume)) < 200, 1, 0)
    rc = fit_regime_curves(volume, price, increasing=True, regime=regime, groups=groups, min_regime_obs=1000)
    assert 0 in rc.by_regime and 1 not in rc.by_regime
    assert rc.curve(1) is rc.pooled
    delta = rc.delta(regime[:5], np.zeros(5), np.full(5, 100.0))
    assert delta.shape == (5,)
    scaled = rc.scaled(0.5)
    assert scaled.pooled.value(100.0) == pytest.approx(0.5 * rc.pooled.value(100.0))
    assert RegimeCurves.from_dict(rc.to_dict()).pooled == rc.pooled


def test_local_hour_regimes_and_hour_groups() -> None:
    times = pd.date_range("2026-03-28T22:00:00Z", periods=16, freq="15min")
    regime = local_hour_regime(times)
    assert regime[0] == 4  # 23:00 local (CET) is in the 22-24 block
    assert regime[-1] == 0  # 01:45 local is in the night block
    groups = hour_groups(times)
    assert len(np.unique(groups)) == 4
    with pytest.raises(ValueError):
        local_hour_regime(times, edges_hours=(1, 24))


def test_alpha_fit_identifies_the_tariff_and_the_previous_quarter_hour() -> None:
    rng = np.random.default_rng(7)
    n = 6000
    si = np.zeros(n)
    for k in range(1, n):
        si[k] = 0.7 * si[k - 1] + rng.normal(0.0, 250.0)
    prev = np.concatenate([[np.nan], si[:-1]])
    tariff = AlphaModel()
    alpha = tariff.predict(si, prev)
    damped = rng.random(n) < 0.2
    alpha = np.where(damped, 0.6 * alpha, alpha)  # a published reduction factor in some quarter-hours
    fit = fit_alpha_model(si, prev, alpha)
    assert fit.model.feature == "mean_abs_si_prev"
    assert fit.model.kind == "tariff" or fit.model.label.startswith("tariff")
    assert fit.zero_rule_share == 1.0
    assert 0.7 < fit.tariff_exact_share < 0.9
    best = fit.candidates[0]
    persistence = [c for c in fit.candidates if c["kind"] == "persistence"][0]
    assert best["change_mae"] < 0.5 * persistence["change_mae"]


def test_pass_through_is_recovered() -> None:
    rng = np.random.default_rng(2)
    n = 5000
    times = pd.date_range("2026-01-01", periods=n, freq="15min", tz="UTC")
    si = np.cumsum(rng.normal(0.0, 80.0, n)) * 0.1 + rng.normal(0.0, 150.0, n)
    net = -0.65 * si + rng.normal(0.0, 10.0, n)
    up, down = np.clip(net, 0.0, None) + 20.0, np.clip(-net, 0.0, None) + 20.0
    assert estimate_pass_through(si, up, down, times) == pytest.approx(0.65, abs=0.03)


def test_error_stats_and_pairs() -> None:
    actual = np.array([1.0, -2.0, 3.0, 0.5])
    stats = error_stats("x", actual, actual * 0.5, np.zeros(4), target_is_change=True)
    assert stats.calibration_slope == pytest.approx(2.0)
    assert stats.r2_vs_baseline == pytest.approx(0.75)
    assert stats.sign_hit_rate == 1.0
    times = pd.date_range("2026-03-02T00:00:00Z", periods=8, freq="15min")
    assert consecutive_pairs(times).tolist() == [1, 2, 3, 5, 6, 7]
    assert consecutive_pairs(times, same_hour=False).tolist() == list(range(1, 8))
    before, after = holdout_split(times, "2026-03-02T01:00:00Z")
    assert len(before) == 4 and len(after) == 4


def test_curve_and_operator_validation_on_a_known_market() -> None:
    market = synthetic_impact_market(days=6, seed=11)
    train, test = (
        market.slice(market.start_utc, market.qh_times[288]),
        market.slice(market.qh_times[288], market.end_utc),
    )
    fit, _ = fit_current_response(train, min_regime_obs=10_000)
    from euroflex_bess_lab.realtime.impact import CurrentFormulaImpact, current_state_from_market

    st = current_state_from_market(test)
    res = validate_curve_changes("mip", st.up_mw, st.mip_eur_mwh, test.qh_times, fit.mip, st.regime)
    assert 0.7 < res.calibration_slope < 1.3
    assert res.r2_vs_baseline > 0.2
    op = CurrentFormulaImpact.from_market(test, fit)
    shift = validate_operator_si_shift("current", op, test.settlement("current"))
    assert shift["all"]["vs_persistence"]["mae"] < shift["all"]["vs_persistence"]["baseline_mae"]
    assert shift["sign_flip"]["vs_persistence"]["n"] > 0
