from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from euroflex_bess_lab.realtime import BatteryParams, IdleStrategy, ThresholdStrategy, simulate
from euroflex_bess_lab.realtime.reserve import (
    ActivationModel,
    ActivationSeries,
    AwardModel,
    CapacityAwards,
    DeliveryBlocks,
    ReservePlan,
    ReserveSettings,
    revenue_stack,
    simulate_reserve,
    synthetic_reserve_inputs,
)
from euroflex_bess_lab.realtime.strategies import BaseStrategy

BATTERY = BatteryParams(power_mw=1.0, energy_mwh=2.0)


def _setup(days: int = 2):
    market, frame = synthetic_reserve_inputs(days=days, seed=7)
    blocks = DeliveryBlocks.regular(market.start_utc, market.end_utc)
    awards = CapacityAwards.from_frame(frame).aligned_to(blocks)
    return market, awards


def _plan(awards: CapacityAwards, up: float, down: float, award_model: AwardModel | None = None, bids=None):
    n = awards.blocks.n
    return ReservePlan.from_offers(
        "test",
        awards,
        BATTERY,
        offered_up_mw=np.full(n, up),
        offered_down_mw=np.full(n, down),
        award_model=award_model,
        bid_up_eur_per_mw_h=None if bids is None else bids,
        bid_down_eur_per_mw_h=None if bids is None else bids,
    )


def _activation(n: int, ratio_up: np.ndarray, ratio_down: np.ndarray, price_up: float, price_down: float):
    return ActivationSeries(
        ratio_up=ratio_up,
        ratio_down=ratio_down,
        price_up=np.full(n, price_up),
        price_down=np.full(n, price_down),
        raw_ratio_up=ratio_up,
        raw_ratio_down=ratio_down,
        model=ActivationModel(pay_as_bid_until_utc=None),
    )


class _Discharge(BaseStrategy):
    name = "discharge"

    def decide(self, ctx) -> float:  # type: ignore[no-untyped-def]
        return 1.0


def test_zero_reservation_reproduces_the_core_simulator() -> None:
    market, awards = _setup()
    zero = ReservePlan.empty(awards.blocks)
    n = market.n_minutes
    act = _activation(n, np.full(n, 0.3), np.full(n, 0.2), 100.0, 10.0)  # activation of 0 MW reserved is 0
    reserve = simulate_reserve(market, ThresholdStrategy(min_history_qh=96), BATTERY, zero, act)
    core = simulate(market, ThresholdStrategy(min_history_qh=96), BATTERY, record_minutes=False)
    for column in ("energy_mwh", "discharged_mwh", "charged_mwh", "revenue_eur", "degradation_eur", "pnl_eur"):
        assert np.allclose(reserve.qh[column], core.qh[column], atol=1e-9), column
    assert reserve.qh["afrr_energy_eur"].abs().sum() == 0.0
    assert reserve.qh["afrr_capacity_eur"].abs().sum() == 0.0


def test_capacity_revenue_is_reserved_mw_times_price_times_hours() -> None:
    market, awards = _setup()
    n = market.n_minutes
    act = _activation(n, np.zeros(n), np.zeros(n), 0.0, 0.0)
    result = simulate_reserve(market, IdleStrategy(), BATTERY, _plan(awards, 0.4, 0.7), act)
    blocks = awards.blocks
    inside = np.asarray((blocks.start_utc >= market.start_utc) & (blocks.end_utc <= market.end_utc))
    expected = float(
        np.sum(
            (0.4 * awards.price("up", "vwap")[inside] + 0.7 * awards.price("down", "vwap")[inside])
            * blocks.hours[inside]
        )
    )
    values = result.block_values()
    assert float(values.loc[inside, "capacity_eur"].sum()) == pytest.approx(expected)
    qh = result.qh
    assert np.allclose(qh["afrr_capacity_up_eur"], 0.4 * qh["afrr_capacity_price_up_eur_mw_h"] * 0.25)
    assert result.pnl_eur == pytest.approx(float(qh["afrr_capacity_eur"].sum()))  # idle, no activation, no wear


def test_clearing_proxies_and_pay_as_bid_award_uncertainty() -> None:
    _, awards = _setup()
    vwap = _plan(awards, 1.0, 1.0, AwardModel("vwap"))
    marginal = _plan(awards, 1.0, 1.0, AwardModel("marginal"))
    assert np.allclose(vwap.price_up_eur_per_mw_h, awards.price("up", "vwap"))
    assert np.allclose(marginal.price_up_eur_per_mw_h, awards.price("up", "marginal"))
    assert np.all(marginal.price_up_eur_per_mw_h >= vwap.price_up_eur_per_mw_h)
    bids = awards.price("up", "marginal").copy()
    bids[::2] += 1.0  # every other bid is priced above the marginal accepted bid
    pab = _plan(awards, 1.0, 1.0, AwardModel("pay_as_bid"), bids=bids)
    assert np.all(pab.up_mw[::2] == 0.0) and np.all(pab.price_up_eur_per_mw_h[::2] == 0.0)
    assert np.all(pab.up_mw[1::2] == 1.0)
    assert np.allclose(pab.price_up_eur_per_mw_h[1::2], bids[1::2])  # paid its own bid
    with pytest.raises(ValueError):
        _plan(awards, 1.0, 1.0, AwardModel("pay_as_bid"))


def test_activated_energy_is_neutral_for_the_brp_perimeter() -> None:
    market, awards = _setup()
    n = market.n_minutes
    rng = np.random.default_rng(0)
    ratio_up = np.where(rng.random(n) < 0.3, rng.random(n), 0.0)
    ratio_down = np.where(ratio_up == 0.0, rng.random(n) * 0.5, 0.0)
    act = _activation(n, ratio_up, ratio_down, 120.0, 30.0)
    result = simulate_reserve(market, IdleStrategy(), BATTERY, _plan(awards, 0.5, 0.5), act)
    qh = result.qh
    assert qh["shortfall_up_mwh"].sum() + qh["shortfall_down_mwh"].sum() == pytest.approx(0.0, abs=1e-12)
    # the battery moved energy, but none of it is imbalance: no double counting with the imbalance price
    assert qh["metered_energy_mwh"].abs().sum() > 1.0
    assert np.allclose(qh["energy_mwh"], 0.0, atol=1e-12)
    assert np.allclose(qh["imbalance_revenue_eur"], 0.0, atol=1e-9)
    assert np.allclose(qh["metered_energy_mwh"], qh["afrr_up_energy_mwh"] - qh["afrr_down_energy_mwh"])
    expected_energy = 0.5 * (ratio_up * 120.0 - ratio_down * 30.0).sum() / 60.0
    assert float(qh["afrr_energy_eur"].sum()) == pytest.approx(expected_energy)
    revenue = qh["afrr_capacity_eur"] + qh["afrr_energy_eur"]
    assert np.allclose(qh["revenue_eur"], revenue)
    assert np.allclose(qh["pnl_eur"], revenue - qh["degradation_eur"])


def test_implicit_energy_is_settled_once_on_top_of_activation() -> None:
    market, awards = _setup()
    n = market.n_minutes
    ratio_down = np.zeros(n)
    ratio_down[:300] = 0.4
    act = _activation(n, np.zeros(n), ratio_down, 0.0, 50.0)
    result = simulate_reserve(market, _Discharge(), BATTERY, _plan(awards, 0.5, 0.5), act)
    qh = result.qh
    price = market.settlement("current")
    assert np.allclose(qh["implicit_revenue_eur"], qh["implicit_energy_mwh"] * price)
    assert np.allclose(qh["energy_mwh"], qh["metered_energy_mwh"] + qh["afrr_down_energy_mwh"])
    assert np.allclose(qh["energy_mwh"], qh["implicit_energy_mwh"])  # fully delivered -> perimeter = implicit only


def test_shortfall_is_settled_at_the_imbalance_price_and_penalised() -> None:
    market, awards = _setup()
    n = market.n_minutes
    battery = BatteryParams(power_mw=1.0, energy_mwh=2.0, initial_soc_frac=0.05)  # empty: cannot deliver upward
    ratio_up = np.zeros(n)
    ratio_up[:30] = 1.0
    act = _activation(n, ratio_up, np.zeros(n), 100.0, 0.0)
    settings = ReserveSettings(restore_power_frac=0.0, shortfall_penalty_eur_mwh=50.0)
    plan = ReservePlan.from_offers(
        "short", awards, battery, offered_up_mw=np.full(awards.blocks.n, 1.0), offered_down_mw=np.zeros(awards.blocks.n)
    )
    requested = simulate_reserve(market, IdleStrategy(), battery, plan, act, settings=settings)
    qh = requested.qh
    price = market.settlement("current")
    assert float(qh["shortfall_up_mwh"].sum()) == pytest.approx(0.5)
    assert float(qh["afrr_energy_eur"].sum()) == pytest.approx(0.5 * 100.0)  # paid on requested energy
    assert np.allclose(qh["shortfall_imbalance_eur"], -qh["shortfall_up_mwh"] * price)
    assert np.allclose(qh["energy_mwh"], -qh["shortfall_up_mwh"])  # the BRP is short by the missing energy
    assert float(qh["penalty_eur"].sum()) == pytest.approx(25.0)
    delivered = simulate_reserve(
        market,
        IdleStrategy(),
        battery,
        plan,
        act,
        settings=ReserveSettings(restore_power_frac=0.0, settle_on="delivered"),
    )
    assert float(delivered.qh["afrr_energy_eur"].sum()) == pytest.approx(0.0)
    assert float(delivered.qh["shortfall_imbalance_eur"].abs().sum()) == 0.0


def test_revenue_stack_components_add_up_to_the_total() -> None:
    market, awards = _setup(days=3)
    n = market.n_minutes
    rng = np.random.default_rng(3)
    act = _activation(n, rng.random(n) * 0.3, rng.random(n) * 0.3, 150.0, 40.0)
    result = simulate_reserve(market, ThresholdStrategy(min_history_qh=96), BATTERY, _plan(awards, 0.5, 0.5), act)
    stack = revenue_stack(result.qh, BATTERY, n_minutes=market.n_minutes)
    parts = (
        stack["afrr_capacity"]
        + stack["afrr_energy"]
        + stack["implicit_balancing"]
        + stack["shortfall_imbalance"]
        + stack["penalty"]
        + stack["degradation"]
    )
    assert parts == pytest.approx(stack["total"])
    years = market.n_minutes / 1440 / 365.25
    assert stack["total"] == pytest.approx(result.pnl_eur / years)
    assert stack["afrr_capacity"] == pytest.approx(stack["afrr_capacity_up"] + stack["afrr_capacity_down"])
    summary = result.summary()
    assert summary["stack_eur_per_mw_year"]["total"] == pytest.approx(stack["total"])
    assert isinstance(result.daily(), pd.DataFrame)
