"""Linear programs of the DE-LU two-stage strategy: feasibility, product blocks, no-trade and oracle bounds."""

from __future__ import annotations

import numpy as np
import pytest

from euroflex_bess_lab.markets.intraday import (
    IntradayDayLP,
    TradingCosts,
    default_battery,
    solve_da_schedule,
    solve_oracle_da_id,
)
from euroflex_bess_lab.markets.intraday.lp import IntradayStep

H = 0.25
BATTERY = default_battery()


def _soc(discharge: np.ndarray, charge: np.ndarray, start: float = 1.0) -> np.ndarray:
    return start + np.cumsum((charge * BATTERY.charge_efficiency - discharge / BATTERY.discharge_efficiency) * H)


def _prices(n: int = 96, seed: int = 0) -> np.ndarray:
    rng = np.random.default_rng(seed)
    t = np.arange(n) / 4.0
    return 80 + 45 * np.cos(2 * np.pi * (t - 19) / 24) - 40 * np.exp(-(((t - 13) / 2.5) ** 2)) + rng.normal(0, 5, n)


def test_da_schedule_is_feasible_profitable_and_flat_within_hourly_products() -> None:
    prices = np.repeat(_prices(24 * 4)[::4], 4)
    blocks = np.repeat(np.arange(24), 4)
    plan = solve_da_schedule(prices, blocks, BATTERY, initial_soc_mwh=1.0, terminal_soc_mwh=1.0, fee_eur_mwh=0.1)
    position = plan.position_mw.reshape(24, 4)
    assert np.allclose(position, position[:, :1]), "hourly products need one position per hour"
    soc = _soc(plan.discharge_mw, plan.charge_mw)
    assert soc.min() >= BATTERY.soc_min_mwh - 1e-7 and soc.max() <= BATTERY.soc_max_mwh + 1e-7
    assert abs(soc[-1] - 1.0) < 1e-6 and np.allclose(soc, plan.soc_end_mwh, atol=1e-6)
    assert (plan.discharge_mw + plan.charge_mw <= BATTERY.max_power_mw + 1e-9).all()
    assert plan.objective_eur > 0
    revenue = float(np.sum(plan.position_mw * prices) * H)
    assert revenue > 0


def test_flat_prices_give_no_trades() -> None:
    plan = solve_da_schedule(
        np.full(96, 70.0), np.arange(96), BATTERY, initial_soc_mwh=1.0, terminal_soc_mwh=1.0, fee_eur_mwh=0.1
    )
    assert np.abs(plan.position_mw).max() < 1e-7


def test_intraday_lp_does_not_trade_when_forecast_equals_the_day_ahead_plan_prices() -> None:
    prices = _prices()
    plan = solve_da_schedule(prices, np.arange(96), BATTERY, initial_soc_mwh=1.0, terminal_soc_mwh=1.0)
    lp = IntradayDayLP(plan.position_mw, BATTERY, TradingCosts(), initial_soc_mwh=1.0, terminal_soc_mwh=1.0)
    lp.set_prices(0, prices)
    lp.solve()
    steps = [lp.step(j) for j in range(96)]
    traded = sum(s.sell_mw + s.buy_mw for s in steps)
    assert traded < 1e-6, "re-trading at the prices the schedule was optimised for only costs fees"


def test_intraday_lp_sells_into_an_expected_spike_and_stays_feasible() -> None:
    prices = _prices()
    plan = solve_da_schedule(prices, np.arange(96), BATTERY, initial_soc_mwh=1.0, terminal_soc_mwh=1.0)
    forecast = prices.copy()
    forecast[40] += 400.0
    lp = IntradayDayLP(plan.position_mw, BATTERY, TradingCosts(), initial_soc_mwh=1.0, terminal_soc_mwh=1.0)
    lp.set_prices(0, forecast)
    lp.solve()
    steps = [lp.step(j) for j in range(96)]
    assert steps[40].sell_mw > 0.5
    d = np.array([s.discharge_mw for s in steps])
    c = np.array([s.charge_mw for s in steps])
    net = np.array([s.sell_mw - s.buy_mw for s in steps])
    assert np.allclose(d - c, plan.position_mw + net, atol=1e-7), "physical = day-ahead position + intraday trades"
    soc = _soc(d, c)
    assert soc.min() >= BATTERY.soc_min_mwh - 1e-7 and soc.max() <= BATTERY.soc_max_mwh + 1e-7
    assert abs(soc[-1] - 1.0) < 1e-6


def test_rolling_commitments_keep_the_lp_feasible() -> None:
    prices = _prices(seed=3)
    plan = solve_da_schedule(prices, np.arange(96), BATTERY, initial_soc_mwh=1.0, terminal_soc_mwh=1.0)
    lp = IntradayDayLP(
        plan.position_mw, BATTERY, TradingCosts(slippage_eur_mwh=0.5), initial_soc_mwh=1.0, terminal_soc_mwh=1.0
    )
    rng = np.random.default_rng(1)
    committed = []
    for k in range(96):
        lp.set_prices(k, prices[k:] + rng.normal(0, 30, 96 - k))
        lp.solve()
        step = lp.step(k)
        lp.fix(k, step)
        committed.append(step)
    d = np.array([s.discharge_mw for s in committed])
    c = np.array([s.charge_mw for s in committed])
    soc = _soc(d, c)
    assert soc.min() >= BATTERY.soc_min_mwh - 1e-6 and soc.max() <= BATTERY.soc_max_mwh + 1e-6
    assert abs(soc[-1] - 1.0) < 1e-5
    assert lp.has_solution


def test_intraday_lp_validates_forecast_length_and_order() -> None:
    lp = IntradayDayLP(np.zeros(8), BATTERY, TradingCosts(), initial_soc_mwh=1.0, terminal_soc_mwh=1.0)
    with pytest.raises(ValueError, match="k..n-1"):
        lp.set_prices(2, np.zeros(8))
    with pytest.raises(RuntimeError, match="solve"):
        lp.step(0)
    lp.fix(0, IntradayStep(0.0, 0.0, 0.0, 0.0))


def test_joint_oracle_bounds_the_day_ahead_oracle() -> None:
    da = _prices(seed=5)
    rng = np.random.default_rng(6)
    idp = da + rng.normal(0, 40, 96)
    costs = TradingCosts()
    blocks = np.arange(96)
    joint = solve_oracle_da_id(da, idp, blocks, BATTERY, costs, initial_soc_mwh=1.0, terminal_soc_mwh=1.0)
    oracle_da = solve_da_schedule(da, blocks, BATTERY, initial_soc_mwh=1.0, terminal_soc_mwh=1.0, fee_eur_mwh=0.1)

    def pnl(x: np.ndarray, sell: np.ndarray, buy: np.ndarray, discharge: np.ndarray) -> float:
        traded = sell + buy
        return float(
            np.sum(x * da) * H
            - np.sum(np.abs(x)) * H * costs.da_fee_eur_mwh
            + np.sum((sell - buy) * idp) * H
            - np.sum(traded) * H * costs.id_cost_eur_mwh
            - np.sum(discharge) * H * BATTERY.degradation_eur_per_mwh
        )

    zero = np.zeros(96)
    joint_value = pnl(joint.position_mw, joint.sell_mw, joint.buy_mw, joint.discharge_mw)
    da_value = pnl(oracle_da.position_mw, zero, zero, oracle_da.discharge_mw)
    assert joint_value >= da_value - 1e-6
    # the day-ahead leg of the oracle is itself a feasible battery schedule
    assert np.all(np.abs(joint.position_mw) <= BATTERY.max_power_mw + 1e-9)
    soc = _soc(joint.discharge_mw, joint.charge_mw)
    assert soc.min() >= BATTERY.soc_min_mwh - 1e-7 and abs(soc[-1] - 1.0) < 1e-6


def test_negative_prices_respect_the_power_rating() -> None:
    prices = np.concatenate([np.full(48, -200.0), np.full(48, 150.0)])
    plan = solve_da_schedule(prices, np.arange(96), BATTERY, initial_soc_mwh=1.0, terminal_soc_mwh=1.0)
    assert (plan.discharge_mw + plan.charge_mw <= BATTERY.max_power_mw + 1e-9).all()
    soc = _soc(plan.discharge_mw, plan.charge_mw)
    assert soc.max() <= BATTERY.soc_max_mwh + 1e-7
