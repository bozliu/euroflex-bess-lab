from __future__ import annotations

import numpy as np
import pytest

from euroflex_bess_lab.realtime import BatteryParams, DecisionContext, PublicationModel
from euroflex_bess_lab.realtime.reserve import (
    ActivationModel,
    ActivationSeries,
    CapacityAwards,
    DeliveryBlocks,
    ReservePlan,
    ReserveSettings,
    ReserveSplitStrategy,
    cap_reservation,
    implicit_power_limits,
    max_symmetric_reserve_mw,
    reserve_is_feasible,
    reserve_soc_band,
    simulate_reserve,
    synthetic_reserve_inputs,
)
from euroflex_bess_lab.realtime.strategies import BaseStrategy

BATTERY = BatteryParams(power_mw=1.0, energy_mwh=2.0)
QUARTER = 0.25


class _Constant(BaseStrategy):
    def __init__(self, power: float) -> None:
        self.power = power
        self.name = "constant"
        self.seen_power: list[float] = []

    def decide(self, ctx: DecisionContext) -> float:
        self.seen_power.append(ctx.battery.max_power_mw)
        return self.power


def _setup(days: int = 1):
    market, frame = synthetic_reserve_inputs(days=days, seed=4)
    blocks = DeliveryBlocks.regular(market.start_utc, market.end_utc)
    awards = CapacityAwards.from_frame(frame).aligned_to(blocks)
    return market, awards


def _plan(awards: CapacityAwards, up: float, down: float) -> ReservePlan:
    n = awards.blocks.n
    return ReservePlan.from_offers(
        "test", awards, BATTERY, offered_up_mw=np.full(n, up), offered_down_mw=np.full(n, down)
    )


def _activation(n: int, ratio_up: np.ndarray | None = None, ratio_down: np.ndarray | None = None) -> ActivationSeries:
    up = np.zeros(n) if ratio_up is None else ratio_up
    down = np.zeros(n) if ratio_down is None else ratio_down
    return ActivationSeries(
        ratio_up=up,
        ratio_down=down,
        price_up=np.full(n, 100.0),
        price_down=np.full(n, 20.0),
        raw_ratio_up=up,
        raw_ratio_down=down,
        model=ActivationModel(pay_as_bid_until_utc=None),
    )


def test_soc_band_and_feasibility_formulas() -> None:
    lo, hi = reserve_soc_band(BATTERY, 0.5, 0.5, QUARTER)
    assert lo == pytest.approx(0.1 + 0.5 * 0.25 / 0.95)
    assert hi == pytest.approx(1.9 - 0.5 * 0.25 * 0.95)
    assert reserve_is_feasible(BATTERY, 1.0, 1.0, QUARTER)
    assert not reserve_is_feasible(BATTERY, 1.2, 0.0, QUARTER)
    # one hour of full activation in both directions needs 1/0.95 + 0.95 MWh per MW > 1.8 MWh usable
    assert not reserve_is_feasible(BATTERY, 1.0, 1.0, 1.0)
    x = max_symmetric_reserve_mw(BATTERY, 1.0)
    assert x == pytest.approx(1.8 / (1 / 0.95 + 0.95))
    assert reserve_is_feasible(BATTERY, x, x, 1.0)
    up, down = cap_reservation(BATTERY, 1.0, 1.0, 1.0)
    assert up == pytest.approx(x) and down == pytest.approx(x)
    assert cap_reservation(BATTERY, 3.0, -1.0, QUARTER) == (1.0, 0.0)
    assert implicit_power_limits(BATTERY, 0.3, 0.6) == pytest.approx((0.7, 0.4))


def test_split_strategy_clips_power_and_energy_headroom() -> None:
    market, awards = _setup()
    n_qh = market.n_qh
    inner = _Constant(5.0)
    split = ReserveSplitStrategy(inner, np.full(n_qh, 0.3), np.full(n_qh, 0.6), sustain_hours=QUARTER)
    ctx = DecisionContext(market, BATTERY, PublicationModel(), "current")
    split.reset(ctx)
    lo, hi = split.band(0)
    assert lo == pytest.approx(0.1 + 0.3 * 0.25 / 0.95)
    assert hi == pytest.approx(1.9 - 0.6 * 0.25 * 0.95)
    ctx.advance(30, 1.0)
    assert split.decide(ctx) == pytest.approx(0.7)  # P - up
    assert inner.seen_power[-1] == pytest.approx(0.7)  # inner sees the free power, not the full battery
    assert ctx.battery is BATTERY  # the view is swapped back
    ctx.advance(31, lo + 0.001)  # only 1 kWh above the reserve floor
    assert split.decide(ctx) == pytest.approx(0.001 * 0.95 * 60)
    inner.power = -5.0
    ctx.advance(32, 1.0)
    assert split.decide(ctx) == pytest.approx(-0.4)  # -(P - down)


def test_split_strategy_restores_soc_into_the_band() -> None:
    market, _ = _setup()
    n_qh = market.n_qh
    split = ReserveSplitStrategy(
        _Constant(0.0), np.full(n_qh, 0.5), np.full(n_qh, 0.5), sustain_hours=QUARTER, restore_power_frac=0.25
    )
    ctx = DecisionContext(market, BATTERY, PublicationModel(), "current")
    split.reset(ctx)
    lo, hi = split.band(0)
    ctx.advance(10, lo - 0.1)
    assert split.decide(ctx) == pytest.approx(-0.25)  # charge at the restoration power
    ctx.advance(11, hi + 0.1)
    assert split.decide(ctx) == pytest.approx(0.25)
    ctx.advance(12, lo - 1e-4)  # tiny gap: restore only what is needed this minute
    assert split.decide(ctx) == pytest.approx(-1e-4 / (0.95 / 60))


def test_full_reservation_leaves_no_implicit_power() -> None:
    market, awards = _setup()
    inner = _Constant(1.0)
    result = simulate_reserve(market, inner, BATTERY, _plan(awards, 1.0, 1.0), _activation(market.n_minutes))
    assert result.qh["implicit_energy_mwh"].abs().max() == 0.0
    assert inner.seen_power == []  # never asked: no free power


def test_activation_is_delivered_for_the_sustain_duration() -> None:
    market, awards = _setup()
    n = market.n_minutes
    ratio_up = np.zeros(n)
    ratio_up[600:615] = 1.0  # 15 minutes of full upward activation after hours of greedy discharge
    battery = BatteryParams(power_mw=1.0, energy_mwh=2.0, initial_soc_frac=0.3)
    result = simulate_reserve(
        market, _Constant(1.0), battery, _plan(awards, 0.5, 0.0), _activation(n, ratio_up), record_minutes=True
    )
    assert result.minute is not None
    soc = result.minute["soc_mwh"].to_numpy()
    lo, _ = reserve_soc_band(battery, 0.5, 0.0, QUARTER)
    assert soc[599] == pytest.approx(lo, abs=1e-9)  # implicit trading stopped at the reserve floor
    assert result.qh["shortfall_up_mwh"].sum() == pytest.approx(0.0, abs=1e-12)
    assert soc.min() >= battery.soc_min_mwh - 1e-12
    delivered = result.minute["afrr_delivered_mw"].to_numpy()[600:615]
    assert np.allclose(delivered, 0.5)


def test_activation_beyond_the_headroom_creates_a_shortfall() -> None:
    market, awards = _setup()
    n = market.n_minutes
    ratio_up = np.zeros(n)
    ratio_up[600:660] = 1.0  # one hour: longer than the 15-minute energy headroom
    battery = BatteryParams(power_mw=1.0, energy_mwh=2.0, initial_soc_frac=0.3)
    settings = ReserveSettings(restore_power_frac=0.0)
    result = simulate_reserve(
        market, _Constant(1.0), battery, _plan(awards, 0.5, 0.0), _activation(n, ratio_up), settings=settings
    )
    shortfall = result.qh["shortfall_up_mwh"].sum()
    assert shortfall == pytest.approx(0.5 * 45 / 60, rel=1e-6)  # the last 45 minutes cannot be delivered
    restored = simulate_reserve(
        market, _Constant(1.0), battery, _plan(awards, 0.5, 0.0), _activation(n, ratio_up), settings=ReserveSettings()
    )
    assert restored.qh["shortfall_up_mwh"].sum() == pytest.approx(0.0, abs=1e-12)  # implicit power refills the SOC
