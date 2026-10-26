"""GB LPs: consistency with the repository's quarter-hour LP, physical limits, response stacking rules."""

from __future__ import annotations

import numpy as np
import pytest

from euroflex_bess_lab.markets.gb.lp import StackProblem, StorageLP, rolling_oracle, solve_stack_lp
from euroflex_bess_lab.markets.gb.rules import (
    DELIVERY_DURATION_HOURS,
    HIGH_PRODUCTS,
    LOW_PRODUCTS,
    RESERVED_CAPACITY_SHARE,
    RESPONSE_PRODUCTS,
    StackingRules,
)
from euroflex_bess_lab.realtime import BatteryParams
from euroflex_bess_lab.realtime.strategies import solve_qh_arbitrage

BATTERY = BatteryParams(power_mw=1.0, energy_mwh=2.0)


def _prices(n: int, seed: int = 0) -> np.ndarray:
    rng = np.random.default_rng(seed)
    return 70 + 35 * np.sin(np.arange(n) * 2 * np.pi / 48) + rng.normal(0, 20, n)


def test_half_hour_lp_matches_repository_quarter_hour_lp_on_duplicated_prices() -> None:
    prices = _prices(96)
    plan = StorageLP(96, BATTERY).solve(prices, soc0=BATTERY.initial_soc_mwh)
    qh_net, _ = solve_qh_arbitrage(np.repeat(prices, 2), BATTERY, initial_soc_mwh=BATTERY.initial_soc_mwh)
    deg = BATTERY.degradation_eur_per_mwh
    value_sp = float((plan.net_mw * prices * 0.5).sum() - (plan.discharge_mw * 0.5 * deg).sum())
    value_qh = float((qh_net * np.repeat(prices, 2) * 0.25).sum() - (np.clip(qh_net, 0, None) * 0.25 * deg).sum())
    assert value_sp == pytest.approx(value_qh, rel=1e-6, abs=1e-6)


def test_storage_lp_respects_soc_power_and_headroom_limits() -> None:
    prices = _prices(48, seed=1)
    up = np.full(48, 0.6)  # e.g. 0.4 MW contracted low-frequency response
    down = np.full(48, 0.8)
    plan = StorageLP(48, BATTERY).solve(prices, soc0=1.0, net_up=up, net_down=down, terminal_value=50.0)
    assert (plan.net_mw <= up + 1e-7).all() and (-plan.net_mw <= down + 1e-7).all()
    assert plan.soc_end_mwh.min() >= BATTERY.soc_min_mwh - 1e-7
    assert plan.soc_end_mwh.max() <= BATTERY.soc_max_mwh + 1e-7
    # SOC dynamics with efficiencies
    soc = 1.0 + np.cumsum(-0.5 * plan.discharge_mw / 0.95 + 0.5 * plan.charge_mw * 0.95)
    assert np.allclose(soc, plan.soc_end_mwh, atol=1e-6)


def test_soft_band_is_respected_when_feasible_and_reported_when_not() -> None:
    lp = StorageLP(8, BATTERY)
    lb = np.full(8, 1.2)
    plan = lp.solve(np.full(8, 100.0), soc0=1.5, soc_lb=lb)
    assert plan.soc_end_mwh.min() >= 1.2 - 1e-7 and plan.soc_violation_mwh == pytest.approx(0.0, abs=1e-7)
    plan = lp.solve(np.full(8, 100.0), soc0=0.2, soc_lb=np.full(8, 1.9))
    assert plan.soc_violation_mwh > 0  # cannot reach the band in time; slack is used instead of failing


def test_deburn_removes_simultaneous_charge_and_discharge() -> None:
    prices = np.full(10, -200.0)  # strongly negative prices reward wasting energy
    raw = StorageLP(10, BATTERY).solve(prices, soc0=1.8)
    assert (np.minimum(raw.discharge_mw, raw.charge_mw) > 1e-6).any()
    clean = StorageLP(10, BATTERY).solve(prices, soc0=1.8, deburn=True)
    assert (np.minimum(clean.discharge_mw, clean.charge_mw) <= 1e-6).all()
    oracle = rolling_oracle(np.concatenate([prices, _prices(40)]), BATTERY, window=24, overlap=8)
    assert (np.minimum(oracle.discharge_mw, oracle.charge_mw) <= 1e-6).all()


@pytest.mark.parametrize("mode", ["import_plus_export", "per_direction", "export_only"])
def test_procurement_rows_follow_the_reserve_reading(mode: str) -> None:
    rules = StackingRules(reserve_mode=mode)  # type: ignore[arg-type]
    rows = rules.procurement_rows(RESPONSE_PRODUCTS, 1.0, 1.0)
    reserve = np.array([1 + RESERVED_CAPACITY_SHARE[p] for p in RESPONSE_PRODUCTS])
    sym_dc = np.array([1 if p in ("DCL", "DCH") else 0 for p in RESPONSE_PRODUCTS], dtype=float)
    feasible = all(float(coef @ (0.9 * sym_dc)) <= rhs + 1e-9 for coef, rhs in rows)
    if mode == "export_only":
        assert not feasible and any(np.allclose(coef, reserve) and rhs == 1.0 for coef, rhs in rows)
    else:
        assert feasible  # 0.9 MW DCL + 0.9 MW DCH with 10 % reserve fits per direction and within 2 MW
    drl_only = np.array([1.0 if p == "DRL" else 0.0 for p in RESPONSE_PRODUCTS])
    drl_ok = all(float(coef @ drl_only) <= rhs + 1e-9 for coef, rhs in rows)
    assert drl_ok == (mode == "import_plus_export")  # 1 MW DRL + 40 % reserve exceeds 1 MW otherwise


def _stack_problem(prices: dict[str, float], n: int = 8, energy: float = 0.0) -> StackProblem:
    return StackProblem(
        price_sell=np.full(n, 80.0),
        price_buy=np.full(n, 80.0),
        free_block=np.zeros(n, dtype=np.int64),
        block_price=[prices],
        block_hours=np.array([n * 0.5]),
        energy={p: np.full(n, energy) for p in RESPONSE_PRODUCTS},
        soc0=1.0,
        degradation=np.full(n, 7.0),
        terminal_value=80.0 * 0.95,
    )


def test_stack_lp_fills_both_directions_within_reserve_and_energy_limits() -> None:
    prices = {p: (10.0 if p in ("DCL", "DCH") else 0.5) for p in RESPONSE_PRODUCTS}
    solution = solve_stack_lp(_stack_problem(prices), BATTERY, StackingRules())
    mw = solution.block_mw[0]
    low = sum(mw[p] * (1 + RESERVED_CAPACITY_SHARE[p]) for p in LOW_PRODUCTS)
    high = sum(mw[p] * (1 + RESERVED_CAPACITY_SHARE[p]) for p in HIGH_PRODUCTS)
    assert low <= 1.0 + 1e-7 and high <= 1.0 + 1e-7
    assert mw["DCL"] == pytest.approx(1 / 1.1, abs=1e-4) and mw["DCH"] == pytest.approx(1 / 1.1, abs=1e-4)
    # response energy volumes fit in the SOC window along the planned trajectory
    rev_low = sum(mw[p] * DELIVERY_DURATION_HOURS[p] for p in LOW_PRODUCTS) / 0.95
    rev_high = sum(mw[p] * DELIVERY_DURATION_HOURS[p] for p in HIGH_PRODUCTS) * 0.95
    assert solution.soc_end_mwh.min() >= BATTERY.soc_min_mwh + rev_low - 1e-6
    assert solution.soc_end_mwh.max() <= BATTERY.soc_max_mwh - rev_high + 1e-6
    net = solution.discharge_mw - solution.charge_mw
    assert (net + sum(mw[p] for p in LOW_PRODUCTS) <= 1.0 + 1e-7).all()


def test_stack_lp_skips_products_without_price_and_values_negative_prices() -> None:
    prices = {p: float("nan") for p in RESPONSE_PRODUCTS}
    prices["DRL"] = 30.0
    solution = solve_stack_lp(_stack_problem(prices), BATTERY, StackingRules(reserve_mode="import_plus_export"))
    assert all(solution.block_mw[0][p] == 0.0 for p in RESPONSE_PRODUCTS if p != "DRL")
    assert solution.block_mw[0]["DRL"] > 0.5
    negative = {p: -5.0 for p in RESPONSE_PRODUCTS}
    solution = solve_stack_lp(_stack_problem(negative), BATTERY)
    assert sum(solution.block_mw[0].values()) == pytest.approx(0.0, abs=1e-7)  # paying with no energy benefit
    # paying 2 GBP/MW/h for DRH is worth it when each MW imports 0.07 MWh per SP that can be sold at 80
    drh = {p: (-2.0 if p == "DRH" else float("nan")) for p in RESPONSE_PRODUCTS}
    solution = solve_stack_lp(_stack_problem(drh, energy=0.07), BATTERY)
    assert solution.block_mw[0]["DRH"] > 0.1
