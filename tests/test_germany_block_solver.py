"""Block-constant German co-optimisation (``solve_germany_day``) against the framework solver."""

from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from euroflex_bess_lab.markets.adapters.germany import GermanBatteryParams, reserve_block_index, solve_germany_day
from euroflex_bess_lab.optimization.solver import (
    solve_day_ahead_afrr_dispatch,
    solve_day_ahead_dispatch,
    solve_day_ahead_fcr_dispatch,
)
from euroflex_bess_lab.types import BatterySpec

TIMESTAMPS = pd.date_range("2025-11-04T23:00Z", periods=96, freq="15min")
BLOCKS = reserve_block_index(TIMESTAMPS)


def _prices() -> np.ndarray:
    hours = np.arange(96) / 4.0
    shape = 95.0 + 55.0 * np.sin((hours - 5.0) / 24.0 * 2 * np.pi) - 70.0 * np.exp(-(((hours - 13.0) / 2.5) ** 2))
    return np.round(shape + 9.0 * np.cos(np.arange(96) * 1.7), 2)


def _block_prices(values: list[float]) -> np.ndarray:
    return np.asarray(values, dtype=float)[BLOCKS]


def _frame(prices: np.ndarray) -> pd.DataFrame:
    return pd.DataFrame({"timestamp_utc": TIMESTAMPS, "resolution_minutes": 15, "price_eur_per_mwh": prices})


SPEC = BatterySpec(
    power_mw=1.0,
    energy_mwh=2.0,
    initial_soc_mwh=1.0,
    terminal_soc_mwh=1.0,
    soc_min_mwh=0.1,
    soc_max_mwh=1.9,
    charge_efficiency=0.95,
    discharge_efficiency=0.95,
)
REPLICATION = GermanBatteryParams(degradation_eur_per_mwh_discharged=0.0, degradation_eur_per_mwh_throughput=4.0)
FCR = _block_prices([9.0, 14.0, 21.0, 12.0, 25.0, 8.0])
UP = _block_prices([4.0, 11.0, 6.0, 3.0, 19.0, 7.0])
DOWN = _block_prices([2.0, 3.0, 15.0, 18.0, 4.0, 1.5])


def test_replication_mode_matches_framework_da_solver() -> None:
    prices = _prices()
    framework = solve_day_ahead_dispatch(_frame(prices), SPEC, degradation_cost_eur_per_mwh=4.0)
    research = solve_germany_day(
        day_ahead_price_eur_per_mwh=prices,
        battery=REPLICATION,
        block_constant_reserve=False,
        soc_reservation_check="start",
    )
    assert research.objective_eur == pytest.approx(framework.objective_value_eur, abs=1e-4)


def test_replication_mode_matches_framework_fcr_solver() -> None:
    prices = _prices()
    framework = solve_day_ahead_fcr_dispatch(
        day_ahead_frame=_frame(prices),
        fcr_capacity_frame=_frame(FCR),
        battery=SPEC,
        degradation_cost_eur_per_mwh=4.0,
        reserve_sustain_duration_minutes=30,
    )
    research = solve_germany_day(
        day_ahead_price_eur_per_mwh=prices,
        battery=REPLICATION,
        block_index=BLOCKS,
        fcr_price_eur_per_mw_h=FCR,
        fcr_energy_hours=0.5,
        block_constant_reserve=False,
        soc_reservation_check="start",
    )
    assert research.objective_eur == pytest.approx(framework.objective_value_eur, abs=1e-4)


def test_replication_mode_matches_framework_capacity_only_afrr_solver() -> None:
    prices = _prices()
    zeros = _frame(np.zeros(96))
    framework = solve_day_ahead_afrr_dispatch(
        day_ahead_frame=_frame(prices),
        afrr_capacity_up_frame=_frame(UP),
        afrr_capacity_down_frame=_frame(DOWN),
        afrr_activation_price_up_frame=zeros,
        afrr_activation_price_down_frame=zeros,
        afrr_activation_ratio_up_frame=zeros,
        afrr_activation_ratio_down_frame=zeros,
        battery=SPEC,
        degradation_cost_eur_per_mwh=4.0,
        reserve_sustain_duration_minutes=60,
    )
    research = solve_germany_day(
        day_ahead_price_eur_per_mwh=prices,
        battery=REPLICATION,
        block_index=BLOCKS,
        afrr_up_price_eur_per_mw_h=UP,
        afrr_down_price_eur_per_mw_h=DOWN,
        afrr_energy_hours=1.0,
        block_constant_reserve=False,
        soc_reservation_check="start",
    )
    assert research.objective_eur == pytest.approx(framework.objective_value_eur, abs=1e-4)


def test_block_constant_reserve_is_constant_per_product_and_never_beats_relaxation() -> None:
    prices = _prices()
    battery = GermanBatteryParams()
    kwargs = {"day_ahead_price_eur_per_mwh": prices, "battery": battery, "block_index": BLOCKS}
    blocked = solve_germany_day(**kwargs, fcr_price_eur_per_mw_h=FCR)
    relaxed = solve_germany_day(**kwargs, fcr_price_eur_per_mw_h=FCR, block_constant_reserve=False)
    for block in range(6):
        values = blocked.fcr_mw[BLOCKS == block]
        assert np.ptp(values) == pytest.approx(0.0, abs=1e-7)
    assert blocked.objective_eur <= relaxed.objective_eur + 1e-6
    afrr = solve_germany_day(**kwargs, afrr_up_price_eur_per_mw_h=UP, afrr_down_price_eur_per_mw_h=DOWN)
    for block in range(6):
        assert np.ptp(afrr.afrr_up_mw[BLOCKS == block]) == pytest.approx(0.0, abs=1e-7)
        assert np.ptp(afrr.afrr_down_mw[BLOCKS == block]) == pytest.approx(0.0, abs=1e-7)
    assert afrr.fcr_mw.max() == pytest.approx(0.0)


def test_energy_buffers_hold_at_both_interval_ends() -> None:
    battery = GermanBatteryParams()
    solution = solve_germany_day(
        day_ahead_price_eur_per_mwh=_prices(),
        battery=battery,
        block_index=BLOCKS,
        afrr_up_price_eur_per_mw_h=UP,
        afrr_down_price_eur_per_mw_h=DOWN,
        afrr_energy_hours=1.0,
    )
    for offset in (0, 1):
        soc = solution.soc_mwh[offset : offset + 96]
        assert (soc - solution.afrr_up_mw * 1.0 / 0.95 >= battery.soc_min_mwh - 1e-6).all()
        assert (soc + solution.afrr_down_mw * 1.0 * 0.95 <= battery.soc_max_mwh + 1e-6).all()
    headroom_up = solution.discharge_mw + solution.afrr_up_mw
    headroom_down = solution.charge_mw + solution.afrr_down_mw
    assert headroom_up.max() <= 1.0 + 1e-6 and headroom_down.max() <= 1.0 + 1e-6
    assert solution.soc_mwh[0] == pytest.approx(1.0) and solution.soc_mwh[-1] == pytest.approx(1.0)
    # 60-minute buffers in both directions must fit into the 1.8 MWh SOC window at the same time.
    combined = solution.afrr_up_mw / 0.95 + solution.afrr_down_mw * 0.95
    assert combined.max() <= battery.soc_max_mwh - battery.soc_min_mwh + 1e-6


def test_negative_prices_never_charge_and_discharge_at_once() -> None:
    prices = _prices()
    prices[40:56] = -120.0
    solution = solve_germany_day(day_ahead_price_eur_per_mwh=prices, battery=GermanBatteryParams())
    assert (np.minimum(solution.charge_mw, solution.discharge_mw) <= 1e-9).all()
    assert solution.charge_mw[40:56].sum() > 0.0


def test_fixed_reserves_are_respected_and_inputs_validated() -> None:
    fixed = _block_prices([0.0, 0.5, 0.5, 0.0, 0.3, 0.0])
    solution = solve_germany_day(
        day_ahead_price_eur_per_mwh=_prices(),
        battery=GermanBatteryParams(),
        block_index=BLOCKS,
        fixed_afrr_up_mw=fixed,
        fixed_afrr_down_mw=np.zeros(96),
    )
    np.testing.assert_allclose(solution.afrr_up_mw, fixed, atol=1e-9)
    assert solution.afrr_down_mw.max() == pytest.approx(0.0)
    with pytest.raises(ValueError, match="shape"):
        solve_germany_day(
            day_ahead_price_eur_per_mwh=_prices(), battery=GermanBatteryParams(), fcr_price_eur_per_mw_h=[1.0]
        )
    with pytest.raises(ValueError, match="non-empty finite"):
        solve_germany_day(day_ahead_price_eur_per_mwh=[np.nan] * 4, battery=GermanBatteryParams())
    with pytest.raises(ValueError, match="SOC bounds"):
        GermanBatteryParams(soc_min_mwh=1.5, soc_max_mwh=1.0)
