from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from euroflex_bess_lab.realtime import BatteryParams, ImpactModel, ThresholdStrategy, simulate
from euroflex_bess_lab.realtime.impact import (
    AlphaModel,
    CurrentFormulaImpact,
    CurrentResponseFit,
    CurrentState,
    NewFormula2026Impact,
    NewFormulaResponseFit,
    NewFormulaState,
    current_price,
    displace,
    fit_current_response,
    fit_new_formula_response,
    new_formula_price,
    operator_for,
)
from euroflex_bess_lab.realtime.impact.fleet import resettle
from euroflex_bess_lab.realtime.impact.response import CurveDelta, NewFormulaCurves
from euroflex_bess_lab.realtime.impact.synthetic import synthetic_impact_market

BATTERY = BatteryParams(power_mw=1.0, energy_mwh=2.0)
Q_GRID = np.linspace(-1500.0, 1500.0, 301)


@pytest.fixture(scope="module")
def market():
    return synthetic_impact_market(days=3, seed=4)


@pytest.fixture(scope="module")
def operators(market):
    fit_c, _ = fit_current_response(market, min_regime_obs=100)
    fit_n = fit_new_formula_response(market, min_regime_obs=100)
    return {
        "current": CurrentFormulaImpact.from_market(market, fit_c),
        "new2026": NewFormula2026Impact.from_market(market, fit_n),
        "current_linear": CurrentFormulaImpact.from_market(market, CurrentResponseFit.linear(0.2, 0.15)),
        "new2026_linear": NewFormula2026Impact.from_market(market, NewFormulaResponseFit.linear(0.2, 0.15)),
    }


def _settlement(market, name: str) -> np.ndarray:
    return market.settlement("new2026" if name.startswith("new2026") else "current")


@pytest.mark.parametrize("name", ["current", "new2026", "current_linear", "new2026_linear"])
def test_zero_deviation_leaves_prices_unchanged(market, operators, name) -> None:
    op = operators[name]
    base = _settlement(market, name).copy()
    base[5] = np.nan
    idx = np.arange(market.n_qh, dtype=np.int64)
    out = op.adjust(idx, base, np.zeros(market.n_qh))
    assert np.array_equal(out, base, equal_nan=True)
    # the model reconstructs the published price exactly at q = 0 (synthetic data obey the formulas)
    rec = op.price_at(0.0)
    assert np.allclose(rec, _settlement(market, name), atol=1e-6)


@pytest.mark.parametrize("name", ["current", "new2026", "current_linear", "new2026_linear"])
def test_more_long_deviation_never_raises_the_price_within_a_branch(operators, name) -> None:
    op = operators[name]
    prices = op.price_at(np.tile(Q_GRID, (len(op), 1)))
    si2 = op.state.si_mw[:, None] + Q_GRID[None, :]
    short = si2 < 0.0
    same_branch = short[:, 1:] == short[:, :-1]
    steps = np.diff(prices, axis=1)
    assert np.all(steps[same_branch] <= 1e-9)
    # and the response is not trivially flat
    assert np.nanmax(np.abs(prices[:, -1] - prices[:, 150])) > 1.0


def _current_state(
    si: float,
    up: float,
    down: float,
    mip: float,
    mdp: float,
    alpha: float,
    floor: float = np.nan,
    cap: float = np.nan,
) -> CurrentState:
    def a(x: float) -> np.ndarray:
        return np.array([x], dtype=float)

    return CurrentState(
        si_mw=a(si),
        si_prev_mw=a(si),
        up_mw=a(up),
        down_mw=a(down),
        mip_eur_mwh=a(mip),
        mdp_eur_mwh=a(mdp),
        alpha_eur_mwh=a(alpha),
        regime=np.zeros(1, dtype=np.int64),
        curve_row=np.zeros(1, dtype=np.int64),
        floor_eur_mwh=a(floor),
        cap_eur_mwh=a(cap),
    )


def test_sign_flip_switches_the_current_formula_branch() -> None:
    fit = CurrentResponseFit.linear(0.1, 0.1)
    tariff = AlphaModel()
    alpha_obs = float(tariff.predict(np.array([-400.0]))[0])
    state = _current_state(-400.0, 420.0, 10.0, 180.0, 40.0, alpha_obs)
    kwargs = {"up": CurveDelta(fit.mip), "down": CurveDelta(fit.mdp), "alpha_model": tariff}
    p0 = current_price(state, np.array([0.0]), **kwargs)[0]
    assert p0 == pytest.approx(180.0 + alpha_obs)
    # still short (SI' = -100): MIP moves down the curve, alpha vanishes below 150 MW
    p_short = current_price(state, np.array([300.0]), **kwargs)[0]
    u2, _ = displace(np.array([420.0]), np.array([10.0]), np.array([300.0]))
    assert p_short == pytest.approx(180.0 + 0.1 * (u2[0] - 420.0))
    # overshoot (SI' = +200 > 150): long branch, MDP minus alpha of the new imbalance
    q = np.array([600.0])
    p_long = current_price(state, q, **kwargs)[0]
    u2, d2 = displace(np.array([420.0]), np.array([10.0]), q)
    # the previous quarter-hour was not affected (q_prev = 0): x' = (|200| + |-400|) / 2
    alpha_new = float(tariff.predict(np.array([200.0]), np.array([-400.0]))[0])
    assert p_long == pytest.approx(40.0 - 0.1 * (d2[0] - 10.0) - alpha_new)
    assert p_long < p_short < p0
    # mirror: a short deviation flips a long system to the short branch
    long_state = _current_state(250.0, 5.0, 260.0, 120.0, 30.0, float(tariff.predict(np.array([250.0]))[0]))
    p_flip = current_price(long_state, np.array([-400.0]), **kwargs)[0]
    assert p_flip > 120.0


def _nf_state(si: float) -> NewFormulaState:
    def a(x: float) -> np.ndarray:
        return np.array([x], dtype=float)

    return NewFormulaState(
        si_mw=a(si),
        afrr_up_mw=a(180.0),
        afrr_down_mw=a(20.0),
        mfrr_up_mw=a(50.0),
        mfrr_down_mw=a(0.0),
        afrr_price_up_eur_mwh=a(110.0),
        afrr_price_down_eur_mwh=a(60.0),
        mfrr_price_up_eur_mwh=a(200.0),
        mfrr_price_down_eur_mwh=a(np.nan),
        weight_afrr=a(0.75),
        weight_mfrr=a(0.25),
        main_up_eur_mwh=a(0.75 * 110.0 + 0.25 * 200.0),
        main_down_eur_mwh=a(0.75 * 60.0),
        incentive_eur_mwh=a(np.nan),
        regime=np.zeros(1, dtype=np.int64),
    )


def test_sign_flip_switches_the_new_formula_branch() -> None:
    curves = NewFormulaResponseFit.linear(0.1, 0.1).curves()
    state = _nf_state(-200.0)
    p0 = new_formula_price(state, np.array([0.0]), curves=curves)[0]
    assert p0 == pytest.approx(0.75 * 110.0 + 0.25 * 200.0)
    p_short = new_formula_price(state, np.array([100.0]), curves=curves)[0]
    assert p_short < p0
    # overshoot: SI' = +100, long branch priced on aFRR down (weights 1 / 0)
    p_long = new_formula_price(state, np.array([300.0]), curves=curves)[0]
    up2, down2 = displace(np.array([230.0]), np.array([20.0]), np.array([300.0]))
    assert p_long == pytest.approx(60.0 - 0.1 * (down2[0] - 20.0))


def test_exact_alpha_rule_and_counterfactual_modes() -> None:
    model = AlphaModel()
    si = np.array([-500.0, -500.0, 300.0, -100.0])
    prev = np.array([-500.0, -300.0, 300.0, -100.0])
    mip = np.array([300.0, 150.0, 120.0, 150.0])
    mdp = np.array([40.0, 40.0, -150.0, 40.0])
    x = 0.5 * (np.abs(si) + np.abs(prev))
    tariff = 200.0 / (1.0 + np.exp((450.0 - x) / 65.0))
    factor = np.array([0.5, 1.0, 0.25, 1.0])  # (400 - MIP)/200 short, (MDP + 200)/200 long, clipped
    expected = np.where(np.abs(si) > 150.0, tariff * factor, 0.0)
    assert np.allclose(model.predict(si, prev, mip=mip, mdp=mdp), expected)
    # exact counterfactual: the rule at the new imbalance and the new marginal prices; zero below 150 MW
    out = model.counterfactual(
        np.zeros(4),
        si,
        prev,
        np.array([-700.0, -120.0, 900.0, -100.0]),
        prev,
        mip_new=np.array([100.0, 150.0, 120.0, 150.0]),
        mdp_new=mdp,
    )
    x2 = 0.5 * (np.array([700.0, 120.0, 900.0, 100.0]) + np.abs(prev))
    t2 = 200.0 / (1.0 + np.exp((450.0 - x2) / 65.0))
    assert np.allclose(out, [t2[0] * 1.0, 0.0, t2[2] * 0.25, 0.0])
    # legacy ratio mode keeps the published / tariff factor
    obs = model.predict(si, prev) * 0.5
    ratio = model.counterfactual(obs, si, prev, si * 1.2, prev, mode="ratio")
    assert np.allclose(ratio[:3], 0.5 * model.predict(si * 1.2, prev)[:3])
    # monotone in |SI'| for a fixed price factor
    grid = -np.linspace(151.0, 1200.0, 50)
    vals = model.counterfactual(
        np.zeros(50), grid, grid, grid, np.full(50, -400.0), mip_new=np.full(50, 250.0), mdp_new=np.zeros(50)
    )
    assert np.all(np.diff(vals) >= -1e-12)  # more negative SI (larger |SI|) -> larger alpha
    with pytest.raises(ValueError):
        model.counterfactual(obs, si, prev, si, prev, mode="nope")


def test_neutral_band_and_floor_cap_rules() -> None:
    fit = CurrentResponseFit.linear(0.1, 0.1)
    kwargs = {"up": CurveDelta(fit.mip), "down": CurveDelta(fit.mdp), "alpha_model": AlphaModel()}
    # short system, FLOOR 100 / CAP 60: midpoint 80 inside |SI'| < 25 MW
    state = _current_state(-40.0, 60.0, 5.0, 130.0, 50.0, 0.0, floor=100.0, cap=60.0)
    p0 = current_price(state, np.array([0.0]), **kwargs)[0]
    assert p0 == pytest.approx(130.0)
    assert current_price(state, np.array([30.0]), **kwargs)[0] == pytest.approx(80.0)  # SI' = -10: neutral band
    p_long = current_price(state, np.array([100.0]), **kwargs)[0]  # SI' = +60: long branch, MDP <= CAP
    assert p_long <= 60.0
    # MIP never falls below FLOOR however much upward activation is displaced
    deep = _current_state(-800.0, 820.0, 0.0, 105.0, 50.0, 0.0, floor=100.0, cap=60.0)
    assert current_price(deep, np.array([600.0]), **kwargs)[0] >= 100.0 - 1e-9
    # observed |SI| < 25: the published price is the midpoint, the structural MIP drives a short deviation
    neutral = _current_state(10.0, 15.0, 20.0, 130.0, 55.0, 0.0, floor=100.0, cap=60.0)
    assert current_price(neutral, np.array([0.0]), **kwargs)[0] == pytest.approx(80.0)
    assert current_price(neutral, np.array([-200.0]), **kwargs)[0] >= 130.0


def test_previous_quarter_hour_deviation_feeds_alpha(market, operators) -> None:
    op = operators["current"]
    si = op.state.si_mw
    k = int(np.flatnonzero((si[1:] < -300.0) & (si[:-1] < -300.0))[0]) + 1
    idx = np.array([k - 1, k], dtype=np.int64)
    base = market.settlement("current")[idx]
    alone = op.adjust(idx[1:], base[1:], np.array([0.0]))
    with_prev = op.adjust(idx, base, np.array([200.0, 0.0]))
    # a long deviation in k-1 shrinks |SI_{k-1}| and therefore alpha (and the price) of quarter-hour k
    assert with_prev[1] < alone[0]


@pytest.mark.parametrize("mechanism", ["current", "new2026"])
def test_operators_are_impact_models_and_plug_into_simulate(market, operators, mechanism) -> None:
    op = operators[mechanism]
    assert isinstance(op, ImpactModel)
    field = "price_cum_eur_mwh" if mechanism == "current" else "nf_imbalance_price_eur_mwh"
    strategy = ThresholdStrategy(signal_field=field, min_history_qh=10_000)
    base = simulate(market, strategy, BATTERY, mechanism=mechanism, record_minutes=True)
    big = simulate(market, strategy, BATTERY, mechanism=mechanism, impact=op, fleet_mw=800.0, record_minutes=False)
    tiny = simulate(market, strategy, BATTERY, mechanism=mechanism, impact=op, fleet_mw=1e-6, record_minutes=False)
    assert big.meta["impact"] == op.name
    assert np.allclose(tiny.qh["price_eur_mwh"], base.qh["price_eur_mwh"], atol=1e-3)
    assert big.pnl_eur < base.pnl_eur
    # re-settling a recorded trace equals simulating with the impact model
    assert base.minute is not None
    again = resettle(
        market,
        BATTERY,
        base.minute["power_mw"].to_numpy(),
        base.minute["soc_mwh"].to_numpy(),
        mechanism=mechanism,
        impact=op,
        fleet_mw=800.0,
    )
    assert np.allclose(again["pnl_eur"], big.qh["pnl_eur"])


def test_aligned_operator_matches_a_slice(market, operators) -> None:
    sub = market.slice(market.qh_times[40], market.qh_times[100])
    for name in ("current", "new2026"):
        op = operators[name]
        aligned = op.aligned(sub)
        q = np.linspace(-300.0, 300.0, sub.n_qh)
        rows = np.arange(40, 100, dtype=np.int64)
        assert np.allclose(aligned.price_at(q), op.price_at(q, rows=rows), equal_nan=True)
    other = synthetic_impact_market(days=1, start="2026-05-01T00:00:00Z", seed=1)
    with pytest.raises(ValueError):
        operators["current"].aligned(other)


def test_operator_for_and_serialisation(market) -> None:
    fit_c, _ = fit_current_response(market, min_regime_obs=100)
    restored = CurrentResponseFit.from_dict(fit_c.to_dict())
    a = operator_for("current", market, fit_c).price_at(250.0)
    b = operator_for("current", market, restored).price_at(250.0)
    assert np.allclose(a, b)
    fit_n = fit_new_formula_response(market, min_regime_obs=100)
    restored_n = NewFormulaResponseFit.from_dict(fit_n.to_dict())
    assert np.allclose(
        operator_for("new2026", market, fit_n).price_at(-250.0),
        operator_for("new2026", market, restored_n).price_at(-250.0),
    )
    with pytest.raises(TypeError):
        operator_for("current", market, fit_n)
    with pytest.raises(KeyError):
        operator_for("unknown", market)


def test_new_formula_curves_object_is_reused(operators) -> None:
    curves = operators["new2026"].fit.curves()
    assert isinstance(curves, NewFormulaCurves)
    assert isinstance(curves.afrr_up, CurveDelta)


def test_pass_through_scales_the_volume_response() -> None:
    state = _current_state(-400.0, 420.0, 10.0, 180.0, 40.0, 0.0)  # no FLOOR / CAP: no neutral band
    fit = CurrentResponseFit.linear(0.1, 0.1)
    kwargs = {"up": CurveDelta(fit.mip), "down": CurveDelta(fit.mdp), "alpha_model": None}
    full = current_price(state, np.array([200.0]), pass_through=1.0, **kwargs)[0]
    half = current_price(state, np.array([200.0]), pass_through=0.5, **kwargs)[0]
    assert 180.0 - full == pytest.approx(2.0 * (180.0 - half))
    with pytest.raises(ValueError):
        displace(np.zeros(1), np.zeros(1), np.zeros(1), pass_through=2.0)


def test_missing_components_keep_the_published_price(market, operators) -> None:
    op = operators["current"]
    broken = pd.Series(op.state.mip_eur_mwh).copy()
    broken.iloc[3] = np.nan
    from dataclasses import replace

    op2 = replace(op, state=replace(op.state, mip_eur_mwh=broken.to_numpy()))
    idx = np.arange(market.n_qh, dtype=np.int64)
    base = market.settlement("current")
    out = op2.adjust(idx, base, np.full(market.n_qh, 300.0))
    if op.state.si_mw[3] + 300.0 < 0.0:
        assert out[3] == base[3]
    assert np.isfinite(out).all()


def test_published_ledgers_are_resettled_like_simulate(market, operators) -> None:
    from euroflex_bess_lab.realtime.impact.fleet import resettle_ledger

    strategy = ThresholdStrategy(min_history_qh=10_000)
    base = simulate(market, strategy, BATTERY, record_minutes=False)
    big = simulate(market, strategy, BATTERY, impact=operators["current"], fleet_mw=600.0, record_minutes=False)
    ledger = base.qh[["timestamp_utc", "energy_mwh", "discharged_mwh", "charged_mwh"]]
    again = resettle_ledger(ledger, market, BATTERY, mechanism="current", impact=operators["current"], fleet_mw=600.0)
    assert np.allclose(again["pnl_eur"], big.qh["pnl_eur"])
    with pytest.raises(ValueError):
        shifted = ledger.assign(timestamp_utc=ledger["timestamp_utc"] + pd.Timedelta(days=30))
        resettle_ledger(shifted, market, BATTERY, mechanism="current", impact=operators["current"], fleet_mw=1.0)


def test_channels_sum_to_the_total_impact(market, operators) -> None:
    from euroflex_bess_lab.realtime.impact.channels import channel_value, current_channels, new_channels

    rng = np.random.default_rng(0)
    q = rng.normal(0.0, 400.0, market.n_qh)
    cur = current_channels(operators["current"], q)
    parts = cur["merit"] + cur["alpha_own"] + cur["alpha_carry"] + cur["branch"]
    assert np.allclose(parts, cur["total"])
    # in quarter-hours that keep their branch, the branch channel is zero
    si = operators["current"].state.si_mw
    keep = np.sign(si + q) == np.sign(si)
    assert np.allclose(cur["branch"][keep], 0.0, atol=1e-9)
    new = new_channels(operators["new2026"], q)
    assert np.allclose(new["merit"] + new["weight"] + new["branch"], new["total"])
    assert np.allclose(new["weight"], 0.0)  # pro-rata displacement keeps the weights
    value = channel_value(cur, q * 0.25 / 100.0, unit_power_mw=1.0, years=1.0)
    total_split = sum(value[f"{c}_eur_per_mw_year"] for c in ("merit", "alpha_own", "alpha_carry", "branch"))
    assert total_split == pytest.approx(value["total_eur_per_mw_year"])


def test_afrr_first_displacement_opens_the_weight_channel() -> None:
    from euroflex_bess_lab.realtime.impact.response import new_formula_parts

    curves = NewFormulaResponseFit.linear(0.0, 0.0).curves()  # isolate the weights: flat product prices
    state = _nf_state(-300.0)  # short; aFRR up 180 @ 110, mFRR up 50 @ 200 (dearer), published weights 0.75/0.25
    pro = new_formula_parts(state, np.array([100.0]), curves=curves, displacement="pro_rata")
    first = new_formula_parts(state, np.array([100.0]), curves=curves, displacement="afrr_first")
    assert pro["weight_up"][0] == 0.0
    # a long deviation displaces aFRR while the dearer mFRR stays: the mFRR weight and the short price rise
    assert first["weight_up"][0] > 0.0
    assert first["main_up2"][0] > pro["main_up2"][0]
    with pytest.raises(ValueError):
        new_formula_parts(state, np.array([1.0]), curves=curves, displacement="nope")
