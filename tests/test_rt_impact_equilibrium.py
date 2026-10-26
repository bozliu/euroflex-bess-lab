from __future__ import annotations

import numpy as np
import pytest

from euroflex_bess_lab.realtime import BatteryParams
from euroflex_bess_lab.realtime.impact import (
    CurrentFormulaImpact,
    NewFormula2026Impact,
    current_state_from_market,
    fit_current_response,
    fit_new_formula_response,
)
from euroflex_bess_lab.realtime.impact.equilibrium import (
    FLEET_POLICIES,
    RunningResponse,
    TablePrice,
    best_response,
    linear_cournot_solution,
    make_fleets,
    nash_equilibrium,
    overshoot_flags,
    simulate_fleet_game,
    symmetric_equilibrium,
)
from euroflex_bess_lab.realtime.impact.policies import VolumeFromSI
from euroflex_bess_lab.realtime.impact.synthetic import synthetic_impact_market

GRID = np.linspace(-1.0, 1.0, 41)
A, B, S, C = 200.0, 0.1, 300.0, 50.0


def linear_price(q):
    return A - B * (S + q)


@pytest.mark.parametrize("n_fleets", [1, 2, 3, 4, 8])
def test_symmetric_equilibrium_matches_linear_cournot(n_fleets: int) -> None:
    cap = np.full(4, 5_000.0)
    q, converged = symmetric_equilibrium(linear_price, n_fleets, cap, np.full(4, C), np.full(4, -1e9), GRID)
    expected = linear_cournot_solution(A, B, S, C, n_fleets)
    assert converged.all()
    assert q == pytest.approx(np.full(4, expected), abs=0.1)
    # the total converges towards the competitive quantity (P = c) as the number of fleets grows
    competitive = (A - B * S - C) / B
    assert n_fleets * expected == pytest.approx(competitive * n_fleets / (n_fleets + 1))


def test_gauss_seidel_nash_matches_cournot_and_handles_heterogeneous_costs() -> None:
    n = 3
    caps = np.full((2, n), 5_000.0)
    q, converged = nash_equilibrium(linear_price, caps, np.full((2, n), C), np.full((2, n), -1e9), GRID)
    assert converged.all()
    assert q[0] == pytest.approx(np.full(n, linear_cournot_solution(A, B, S, C, n)), abs=0.2)
    # heterogeneous marginal costs: q_i = (a - b s - (n + 1) c_i + sum c) / (b (n + 1))
    costs = np.array([[40.0, 50.0, 60.0]])
    q2, conv2 = nash_equilibrium(linear_price, np.full((1, n), 5_000.0), costs, np.full((1, n), -1e9), GRID)
    expected = (A - B * S - (n + 1) * costs[0] + costs[0].sum()) / (B * (n + 1))
    assert conv2.all()
    assert q2[0] == pytest.approx(expected, abs=0.3)


def test_best_response_respects_capacity_and_profitability() -> None:
    cap = np.array([50.0, 5_000.0, 5_000.0])
    cost_up = np.array([C, C, 1_000.0])
    q = best_response(lambda x: linear_price(x), cap, cost_up, np.full(3, -1e9), GRID)
    assert q[0] == pytest.approx(50.0)  # capacity binds
    assert q[1] == pytest.approx((A - B * S - C) / (2 * B), abs=0.1)  # interior monopoly quantity
    assert q[2] == 0.0  # never profitable
    # charging side: a price far below the charge band makes the fleet charge
    q_ch = best_response(lambda x: -50.0 - 0.1 * x, np.array([100.0]), np.array([500.0]), np.array([0.0]), GRID)
    assert q_ch[0] < 0.0


def test_table_price_interpolates_exactly_for_linear_prices() -> None:
    cap = np.array([100.0, 400.0])
    grid = np.linspace(-1, 1, 11)[None, :] * cap[:, None]
    table = TablePrice(-cap, 2 * cap / 10, linear_price(grid))
    q = np.array([[-37.0, 0.0, 91.0], [-400.0, 13.0, 399.0]])
    assert np.allclose(table(q), linear_price(q))
    assert np.allclose(table(np.array([5.0, -5.0])), linear_price(np.array([5.0, -5.0])))


def test_overshoot_flags() -> None:
    si = np.array([-200.0, -200.0, 100.0, 50.0, 0.0])
    q = np.array([100.0, 300.0, 50.0, -500.0, 0.0])
    flags = overshoot_flags(si, q)
    assert flags["overshoot"].tolist() == [False, True, False, True, False]
    assert flags["harmful"].tolist() == [False, False, True, True, False]
    assert flags["active"].tolist() == [True, True, True, True, False]


@pytest.fixture(scope="module")
def game_setup():
    market = synthetic_impact_market(days=2, seed=5)
    fit_c, _ = fit_current_response(market, min_regime_obs=100)
    fit_n = fit_new_formula_response(market, min_regime_obs=100)
    st = current_state_from_market(market)
    vmap = VolumeFromSI.fit(st.si_mw, st.up_mw, st.down_mw)
    return market, fit_c, fit_n, vmap


@pytest.mark.parametrize("mechanism", ["current", "new2026"])
def test_fleet_game_herding_overshoots_more_as_capacity_grows(game_setup, mechanism) -> None:
    market, fit_c, fit_n, vmap = game_setup
    fit = fit_c if mechanism == "current" else fit_n
    settle = (
        CurrentFormulaImpact.from_market(market, fit_c)
        if mechanism == "current"
        else NewFormula2026Impact.from_market(market, fit_n)
    )
    running = RunningResponse(mechanism, market, fit, volume_map=vmap)
    cost_op = CurrentFormulaImpact.from_market(market, fit_c)
    battery = BatteryParams(power_mw=1.0, energy_mwh=2.0)
    caps = (50.0, 1_500.0)
    out = {}
    for policy in ("herding", "central"):
        scenarios = [make_fleets(q, 4, heterogeneous=policy == "herding") for q in caps]
        res = simulate_fleet_game(
            market,
            mechanism,
            policy=policy,
            scenarios=scenarios,
            settle=settle,
            running=running,
            cost_operator=cost_op,
            battery=battery,
            min_history_qh=8,
        )
        out[policy] = res
        assert [r.total_mw for r in res] == list(caps)
        assert {"eur_per_mw_year", "overshoot_share", "activation_cost_change_pct"} <= set(res[0].summary)
        assert len(res[0].qh) == market.n_qh and len(res[0].fleets) == 4
    herd_small, herd_big = out["herding"]
    assert herd_big.summary["overshoot_share"] > herd_small.summary["overshoot_share"]
    assert herd_big.summary["eur_per_mw_year"] < herd_small.summary["eur_per_mw_year"]
    # a joint-profit optimiser never earns less per MW than herding fleets of the same total size
    assert out["central"][1].summary["eur_per_mw_year"] >= herd_big.summary["eur_per_mw_year"] - 1.0


def test_fleet_game_rejects_bad_input(game_setup) -> None:
    market, fit_c, _, vmap = game_setup
    settle = CurrentFormulaImpact.from_market(market, fit_c)
    running = RunningResponse("current", market, fit_c, volume_map=vmap)
    with pytest.raises(KeyError):
        simulate_fleet_game(
            market,
            "current",
            policy="nope",
            scenarios=[make_fleets(10, 2, heterogeneous=False)],
            settle=settle,
            running=running,
        )
    with pytest.raises(ValueError):
        simulate_fleet_game(
            market,
            "current",
            policy="herding",
            scenarios=[make_fleets(10, 2, heterogeneous=False), make_fleets(10, 3, heterogeneous=False)],
            settle=settle,
            running=running,
        )
    assert set(FLEET_POLICIES) == {"herding", "herding_homogeneous", "naive", "nash", "central"}
    with pytest.raises(TypeError):
        RunningResponse("current", market, fit_c)


def test_running_response_is_exact_at_zero(game_setup) -> None:
    market, fit_c, fit_n, vmap = game_setup
    for mechanism, fit in (("current", fit_c), ("new2026", fit_n)):
        running = RunningResponse(mechanism, market, fit, volume_map=vmap)
        r = 500
        at_zero = running.price(r, np.zeros(3), np.zeros(3))
        assert np.allclose(at_zero, running.base[r])
        grid = np.linspace(-800.0, 800.0, 33)[None, :].repeat(2, axis=0)
        prices = running.price(r, grid, np.zeros(2))
        assert prices.shape == (2, 33)
