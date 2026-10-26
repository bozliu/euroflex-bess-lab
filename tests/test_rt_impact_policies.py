from __future__ import annotations

import numpy as np
import pytest

from euroflex_bess_lab.realtime import BatteryParams, PublicationModel, ThresholdStrategy, check_no_lookahead, simulate
from euroflex_bess_lab.realtime.impact import (
    CurrentFormulaImpact,
    NewFormula2026Impact,
    current_state_from_market,
    fit_current_response,
    fit_new_formula_response,
)
from euroflex_bess_lab.realtime.impact.fleet import CURVE_COLUMNS, PolicySpec, capacity_value_curve, curve_sidecar
from euroflex_bess_lab.realtime.impact.policies import (
    ImpactAwareThresholdStrategy,
    VolumeFromSI,
    price_taking_threshold,
    threshold_bands,
)
from euroflex_bess_lab.realtime.impact.synthetic import synthetic_impact_market

BATTERY = BatteryParams(power_mw=1.0, energy_mwh=2.0)
PUB = PublicationModel()


@pytest.fixture(scope="module")
def setup():
    market = synthetic_impact_market(days=3, seed=9, mip_slope=0.3, mdp_slope=0.25, afrr_slope=0.3)
    fit_c, _ = fit_current_response(market, min_regime_obs=100)
    fit_n = fit_new_formula_response(market, min_regime_obs=100)
    st = current_state_from_market(market)
    vmap = VolumeFromSI.fit(st.si_mw, st.up_mw, st.down_mw)
    ops = {
        "current": CurrentFormulaImpact.from_market(market, fit_c),
        "new2026": NewFormula2026Impact.from_market(market, fit_n),
    }
    return market, {"current": fit_c, "new2026": fit_n}, vmap, ops


@pytest.mark.parametrize("mechanism", ["current", "new2026"])
def test_zero_believed_impact_reproduces_the_threshold_rule(setup, mechanism) -> None:
    market, fits, vmap, _ = setup
    base = simulate(market, price_taking_threshold(mechanism, min_history_qh=20), BATTERY, mechanism=mechanism)
    zero = simulate(
        market,
        ImpactAwareThresholdStrategy(
            mechanism, fits[mechanism], 500.0, volume_map=vmap, impact_scale=0.0, min_history_qh=20
        ),
        BATTERY,
        mechanism=mechanism,
    )
    assert base.minute is not None and zero.minute is not None
    assert np.array_equal(base.minute["power_mw"].to_numpy(), zero.minute["power_mw"].to_numpy())


def test_threshold_bands_match_the_threshold_strategy(setup) -> None:
    market, _, _, _ = setup
    strategy = ThresholdStrategy(min_history_qh=20, lookback_qh=96)
    from euroflex_bess_lab.realtime.replay import DecisionContext

    ctx = DecisionContext(market, BATTERY, PUB, "current")
    strategy.reset(ctx)
    lower, upper = threshold_bands(
        market.settlement("current"),
        market.n_qh,
        PUB,
        lookback_qh=96,
        upper_quantile=0.75,
        lower_quantile=0.25,
        min_spread_eur_mwh=20.0,
        min_history_qh=20,
        default_upper_eur_mwh=150.0,
        default_lower_eur_mwh=0.0,
    )
    for k in (0, 5, 30, 150, market.n_qh - 1):
        ctx.advance(15 * k, BATTERY.initial_soc_mwh)
        lo, up = strategy._bands(ctx)
        assert lo == pytest.approx(lower[k]) and up == pytest.approx(upper[k])


@pytest.mark.parametrize("mechanism", ["current", "new2026"])
def test_impact_aware_policy_passes_the_lookahead_audit(setup, mechanism) -> None:
    market, fits, vmap, _ = setup
    audit = check_no_lookahead(
        lambda: ImpactAwareThresholdStrategy(mechanism, fits[mechanism], 400.0, volume_map=vmap, min_history_qh=20),
        market,
        BATTERY,
        cut_minute=1500,
        mechanism=mechanism,
        publication=PUB,
    )
    assert audit.passed, audit


@pytest.mark.parametrize("mechanism", ["current", "new2026"])
def test_impact_aware_policy_throttles_and_beats_herding_for_a_large_fleet(setup, mechanism) -> None:
    market, fits, vmap, ops = setup
    q = 1500.0
    herd = simulate(
        market,
        price_taking_threshold(mechanism, min_history_qh=20),
        BATTERY,
        mechanism=mechanism,
        impact=ops[mechanism],
        fleet_mw=q,
        record_minutes=False,
    )
    aware = simulate(
        market,
        ImpactAwareThresholdStrategy(mechanism, fits[mechanism], q, volume_map=vmap, min_history_qh=20),
        BATTERY,
        mechanism=mechanism,
        impact=ops[mechanism],
        fleet_mw=q,
        record_minutes=False,
    )
    assert aware.pnl_eur > herd.pnl_eur
    assert aware.qh["discharged_mwh"].sum() < herd.qh["discharged_mwh"].sum()


def test_policy_validation_errors(setup) -> None:
    _, fits, vmap, _ = setup
    with pytest.raises(TypeError):
        ImpactAwareThresholdStrategy("current", fits["new2026"], 10.0, volume_map=vmap)
    with pytest.raises(ValueError):
        ImpactAwareThresholdStrategy("current", fits["current"], 10.0)
    with pytest.raises(ValueError):
        ImpactAwareThresholdStrategy("new2026", fits["new2026"], 0.0)
    with pytest.raises(ValueError):
        ImpactAwareThresholdStrategy("new2026", fits["new2026"], 10.0, grid_points=4)


def test_volume_map_is_monotone_and_follows_the_imbalance(setup) -> None:
    _, _, vmap, _ = setup
    si = np.linspace(-1500.0, 1500.0, 61)
    up, down = vmap(si)
    assert np.all(np.diff(up) <= 1e-9) and np.all(np.diff(down) >= -1e-9)
    assert up[0] > 1000.0 and down[-1] > 1000.0


def test_capacity_value_curve_shape_and_sidecar(setup) -> None:
    market, fits, vmap, ops = setup
    mechanism = "current"
    policies = [
        PolicySpec("price_taking_threshold", lambda q: price_taking_threshold(mechanism, min_history_qh=20), False),
        PolicySpec(
            "impact_aware_threshold",
            lambda q: ImpactAwareThresholdStrategy(mechanism, fits[mechanism], q, volume_map=vmap, min_history_qh=20),
            True,
        ),
    ]
    sizes = (1.0, 200.0, 1600.0)
    frame, ledgers = capacity_value_curve(
        market,
        mechanism,
        ops[mechanism],
        policies,
        si_mw=current_state_from_market(market).si_mw,
        battery=BATTERY,
        window="synthetic",
        fleet_sizes=sizes,
        keep_ledgers_for=(200.0,),
    )
    assert set(CURVE_COLUMNS) <= set(frame.columns)
    assert len(frame) == 2 * len(sizes)
    pt = frame[frame["policy"] == "price_taking_threshold"].set_index("fleet_mw")
    ia = frame[frame["policy"] == "impact_aware_threshold"].set_index("fleet_mw")
    assert pt.loc[1600.0, "eur_per_mw_year"] < pt.loc[1.0, "eur_per_mw_year"]
    assert pt.loc[1600.0, "overshoot_share"] > pt.loc[1.0, "overshoot_share"]
    assert ia.loc[1600.0, "eur_per_mw_year"] > pt.loc[1600.0, "eur_per_mw_year"]
    assert len(ledgers) == 2 and {"strategy", "mechanism", "fleet_mw"} <= set(ledgers[0].columns)
    sidecar = curve_sidecar(frame, battery=BATTERY, publication=PUB, settings={"test": True})
    assert sidecar["table"] == "impact_capacity_value_curve"
    assert set(CURVE_COLUMNS) <= set(sidecar["columns"])
