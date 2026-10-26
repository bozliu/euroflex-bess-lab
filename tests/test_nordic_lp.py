"""Nordic day LP: product reservations, blocks, hourly day-ahead products, settlement and consistency with the
real-time engine's ``solve_qh_arbitrage``."""

from __future__ import annotations

from dataclasses import replace

import numpy as np
import pytest

from euroflex_bess_lab.markets.nordic.lp import DayInputs, check_schedule, settle_day, solve_day
from euroflex_bess_lab.markets.nordic.rules import (
    AFRR_UP,
    FCR_D_UP,
    FCR_N,
    GATE_CLOSURES,
    PRODUCTS,
    RULE_SOURCES,
    ZONE_PRODUCTS,
    zone_products,
)
from euroflex_bess_lab.realtime import BatteryParams
from euroflex_bess_lab.realtime.strategies import solve_qh_arbitrage

BATTERY = BatteryParams(power_mw=1.0, energy_mwh=2.0)
N = 96
QH_HOUR = np.arange(N) // 4


def _inputs(prices: dict[str, float], da: np.ndarray | None = None, **kwargs: object) -> DayInputs:
    return DayInputs(
        da_price=np.full(N, 50.0) if da is None else da,
        qh_hour=QH_HOUR,
        reserve_price={k: np.full(24, v) for k, v in prices.items()},
        **kwargs,  # type: ignore[arg-type]
    )


def test_rules_table_matches_published_reservations() -> None:
    assert (FCR_N.power_up, FCR_N.power_down, FCR_N.energy_up_hours, FCR_N.energy_down_hours) == (1.34, 1.34, 1.0, 1.0)
    assert (FCR_D_UP.power_up, FCR_D_UP.power_down, FCR_D_UP.energy_up_hours) == (1.0, 0.2, pytest.approx(1 / 3))
    assert PRODUCTS["fcr_dk1"].block_hours == 4 and PRODUCTS["fcr_dk1"].power_up == 1.25
    dk = {p.key: p for p in zone_products("DK2")}
    assert dk["afrr_up"].power_down == 1.0 and dk["mfrr_down"].power_up == 1.0  # Energinet opposite reservation
    se = {p.key: p for p in zone_products("SE3")}
    assert se["afrr_up"].power_down == 0.0
    assert set(ZONE_PRODUCTS["FI"]) == {"afrr_up", "afrr_down"}
    assert all(url.startswith("https://") for url in RULE_SOURCES.values())
    assert any("00:30" in g.gate_closure for g in GATE_CLOSURES) and any(
        "07:30" in g.gate_closure for g in GATE_CLOSURES
    )


@pytest.mark.parametrize(
    ("zone", "prices", "expected"),
    [
        ("SE3", {"fcr_n": 10.0}, {"fcr_n": 1 / 1.34}),  # NEM headroom 0.34 C binds the power
        ("SE3", {"fcr_d_up": 10.0, "fcr_d_down": 10.0}, {"fcr_d_up": 1 / 1.2, "fcr_d_down": 1 / 1.2}),
        ("SE3", {"fcr_d_up": 10.0}, {"fcr_d_up": 1.0}),
        ("DK1", {"fcr_dk1": 10.0}, {"fcr_dk1": 0.8}),
        # aFRR 1 h each way from 50 % SOC: energy binds (0.9 MWh stored above the floor / below the ceiling).
        ("SE3", {"afrr_up": 10.0, "afrr_down": 10.0}, {"afrr_up": 0.9 * 0.95, "afrr_down": 0.9 / 0.95}),
        # In DK the opposite-direction reservation makes up + down share the 1 MW.
        ("DK2", {"afrr_up": 10.0, "afrr_down": 10.0}, {"afrr_total": 1.0}),
    ],
)
def test_single_product_capacity_matches_reservation_rules(
    zone: str, prices: dict[str, float], expected: dict[str, float]
) -> None:
    products = zone_products(zone, tuple(prices))
    inputs = _inputs(prices)
    schedule = solve_day(inputs, products, BATTERY)
    assert check_schedule(schedule, inputs, products, BATTERY) == []
    if "afrr_total" in expected:
        total = schedule.reserve_mw["afrr_up"] + schedule.reserve_mw["afrr_down"]
        assert np.allclose(total, expected["afrr_total"], atol=1e-6)
        return
    for key, value in expected.items():
        assert np.allclose(schedule.reserve_mw[key], value, atol=1e-6)


def test_dk1_fcr_is_constant_within_four_hour_blocks_including_a_25_hour_day() -> None:
    n = 100  # 25-hour day: local hours 00, 01, 02, 02, 03, ... -> block 0 has five hours
    local_hours = np.r_[0, 1, 2, 2, np.arange(3, 24)]
    blocks = np.unique(local_hours // 4, return_inverse=True)[1]
    prices = np.where(np.arange(25) < 5, 1.0, 30.0)
    prices[2] = 500.0  # one expensive hour inside block 0 lifts the whole block
    inputs = DayInputs(
        da_price=np.full(n, 50.0), qh_hour=np.arange(n) // 4, reserve_price={"fcr_dk1": prices}, hour_block={4: blocks}
    )
    schedule = solve_day(inputs, zone_products("DK1", ("fcr_dk1",)), BATTERY)
    mw = schedule.reserve_mw["fcr_dk1"]
    assert len(mw) == 25
    for block in np.unique(blocks):
        assert np.ptp(mw[blocks == block]) < 1e-9
    assert mw[0] == pytest.approx(0.8)


def test_hourly_day_ahead_products_keep_net_power_constant_within_each_hour() -> None:
    rng = np.random.default_rng(3)
    da = np.repeat(rng.normal(60.0, 30.0, 24), 4) + np.tile([0.0, 5.0, -5.0, 0.0], 24)
    schedule = solve_day(DayInputs(da, QH_HOUR, hourly_da=True), [], BATTERY)
    net = schedule.net_mw.reshape(24, 4)
    assert np.abs(net - net[:, :1]).max() < 1e-7
    free = solve_day(DayInputs(da, QH_HOUR, hourly_da=False), [], BATTERY)
    assert free.objective_eur >= schedule.objective_eur - 1e-9


def test_terminal_soc_missing_prices_and_untradable_quarter_hours() -> None:
    da = np.r_[np.full(48, 10.0), np.full(48, 150.0)]
    prices = {"afrr_up": np.r_[np.full(12, np.nan), np.full(12, 20.0)]}
    tradable = np.ones(N, dtype=bool)
    tradable[60:64] = False
    inputs = DayInputs(da, QH_HOUR, reserve_price=prices, da_tradable=tradable)
    schedule = solve_day(inputs, zone_products("SE3", ("afrr_up",)), BATTERY)
    assert schedule.soc_mwh[-1] == pytest.approx(BATTERY.initial_soc_mwh)
    assert np.allclose(schedule.reserve_mw["afrr_up"][:12], 0.0)  # no market in those hours
    assert np.allclose(schedule.charge_mw[60:64], 0.0) and np.allclose(schedule.discharge_mw[60:64], 0.0)


def test_settlement_pays_realised_prices_and_nothing_where_the_market_was_missing() -> None:
    products = zone_products("SE3", ("fcr_d_up",))
    forecast = _inputs({"fcr_d_up": 10.0})
    schedule = solve_day(forecast, products, BATTERY)
    realised = _inputs({"fcr_d_up": 4.0})
    realised.reserve_price = {"fcr_d_up": np.r_[np.full(12, np.nan), np.full(12, 4.0)]}
    settled = settle_day(schedule, realised, BATTERY)
    assert settled.reserve_revenue_eur["fcr_d_up"] == pytest.approx(12 * 4.0 * 1.0)
    assert settled.net_eur == pytest.approx(
        settled.da_revenue_eur + settled.reserve_total_eur - settled.degradation_eur
    )


def test_da_only_lp_reproduces_realtime_solve_qh_arbitrage() -> None:
    rng = np.random.default_rng(11)
    for _ in range(5):
        prices = 70 + 40 * np.sin(np.arange(N) / N * 4 * np.pi) + rng.normal(0, 20, N)
        ours = solve_day(DayInputs(prices, QH_HOUR), [], BATTERY, free_end_soc=True, converter_limit=False)
        net, _ = solve_qh_arbitrage(prices, BATTERY, initial_soc_mwh=BATTERY.initial_soc_mwh)
        d, c = np.maximum(net, 0.0), np.maximum(-net, 0.0)
        theirs = np.sum(prices * net) * 0.25 - BATTERY.degradation_eur_per_mwh * d.sum() * 0.25 - 1e-3 * c.sum() * 0.25
        assert ours.objective_eur == pytest.approx(theirs, abs=1e-6)


def test_full_stack_schedules_pass_the_independent_rule_check_on_random_days() -> None:
    rng = np.random.default_rng(5)
    for zone in ("DK1", "DK2", "SE4", "FI"):
        products = zone_products(zone)
        for _ in range(3):
            prices = {p.key: rng.gamma(2.0, 8.0, 24) for p in products}
            inputs = _inputs({}, da=rng.normal(70, 50, N))
            inputs.reserve_price = prices
            inputs.hour_block = {4: np.arange(24) // 4}
            schedule = solve_day(inputs, products, BATTERY)
            assert check_schedule(schedule, inputs, products, BATTERY) == []
            assert schedule.soc_mwh.min() >= BATTERY.soc_min_mwh - 1e-7


def test_larger_fcr_n_energy_requirement_never_increases_value() -> None:
    inputs = _inputs({"fcr_n": 25.0, "afrr_up": 5.0})
    base = solve_day(inputs, zone_products("SE3", ("fcr_n", "afrr_up")), BATTERY)
    stricter = [replace(FCR_N, energy_up_hours=1.5, energy_down_hours=1.5), zone_products("SE3", ("afrr_up",))[0]]
    tight = solve_day(inputs, stricter, BATTERY)
    assert tight.objective_eur <= base.objective_eur + 1e-9
    assert AFRR_UP.energy_up_hours == 1.0
