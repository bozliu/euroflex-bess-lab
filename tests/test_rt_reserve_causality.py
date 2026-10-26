from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from euroflex_bess_lab.realtime import BatteryParams, PublicationModel, ThresholdStrategy, check_no_lookahead
from euroflex_bess_lab.realtime.reserve import (
    SYMMETRIC_CANDIDATES,
    CapacityAwards,
    DeliveryBlocks,
    ForecastSplitPolicy,
    ReservePlan,
    ReserveSplitStrategy,
    ReserveStudyConfig,
    build_activation,
    check_allocation_causality,
    check_reserve_no_lookahead,
    forecast_capacity_prices,
    run_policy_suite,
    synthetic_reserve_inputs,
)
from euroflex_bess_lab.realtime.strategies import BaseStrategy

BATTERY = BatteryParams(power_mw=1.0, energy_mwh=2.0)
PUBLICATION = PublicationModel(minute_lag=1, qh_final_lag_minutes=5)


def _inner() -> ThresholdStrategy:
    return ThresholdStrategy(min_history_qh=8, lookback_qh=96)


def _setup(days: int = 6):
    # low capacity prices so that reserving and trading implicitly compete and the forecast policy switches
    market, frame = synthetic_reserve_inputs(days=days, seed=11, capacity_price_up=4.0, capacity_price_down=3.0)
    blocks = DeliveryBlocks.regular(market.start_utc, market.end_utc)
    awards = CapacityAwards.from_frame(frame).aligned_to(blocks)
    return market, awards


def _builder(policy: str):
    config = ReserveStudyConfig(forecast=ForecastSplitPolicy(lookback_days=3, min_history=1))

    def build(market, awards) -> ReservePlan:  # type: ignore[no-untyped-def]
        suite = run_policy_suite(
            market, awards, make_inner=_inner, config=config, publication=PUBLICATION, policies=[policy]
        )
        return suite.results[policy].plan

    return build


def test_forecast_allocation_is_causal_and_oracle_is_not() -> None:
    market, awards = _setup()
    gate = awards.blocks.gate_closure_utc
    cut = pd.Timestamp(gate[np.flatnonzero(gate > market.start_utc + pd.Timedelta(days=3))[0]])
    audit = check_allocation_causality(_builder("forecast"), market, awards, cut_utc=cut, publication=PUBLICATION)
    assert audit.passed, audit
    assert audit.decided_blocks > 12
    plan = _builder("forecast")(market, awards)
    assert len(set(np.round(plan.offered_up_mw, 9))) > 1, "test needs a policy that actually switches"
    cheat = check_allocation_causality(_builder("oracle"), market, awards, cut_utc=cut, publication=PUBLICATION)
    assert not cheat.passed


def test_capacity_forecast_only_uses_published_auctions() -> None:
    blocks = DeliveryBlocks.regular("2026-03-01T00:00:00Z", "2026-03-08T00:00:00Z")
    day = np.asarray((blocks.delivery_day - blocks.delivery_day[0]) / pd.Timedelta(days=1), dtype=float)
    price = 10.0 * day + blocks.block_of_day  # encodes (delivery day, block of day)
    awards = CapacityAwards(
        blocks=blocks,
        awarded_mw={"up": np.full(blocks.n, 100.0), "down": np.full(blocks.n, 100.0)},
        vwap_eur_per_mw_h={"up": price, "down": price},
        marginal_eur_per_mw_h={"up": price, "down": price},
    )
    forecast = forecast_capacity_prices(awards, lookback_days=1)["up"]
    later = np.flatnonzero(day >= 2)
    assert np.allclose(forecast[later], price[later] - 10.0)  # same block, previous delivery day
    assert np.isnan(forecast[day == 0]).all()
    window = forecast_capacity_prices(awards, lookback_days=3)["up"]
    i = later[-1]
    assert window[i] == pytest.approx(price[i] - 20.0)  # mean of D-1, D-2, D-3


def test_perturbation_only_touches_unpublished_values() -> None:
    from euroflex_bess_lab.realtime.reserve.allocation import _perturb_awards, _perturb_market

    market, awards = _setup(days=3)
    cut = pd.Timestamp(awards.blocks.gate_closure_utc[12])
    cut_minute = int((cut - market.start_utc) / pd.Timedelta(minutes=1))
    perturbed = _perturb_awards(awards, cut, seed=1)
    published = np.asarray(awards.blocks.results_published_utc <= cut)
    assert published.any() and (~published).any()
    for direction in ("up", "down"):
        assert np.array_equal(perturbed.awarded_mw[direction][published], awards.awarded_mw[direction][published])
        assert np.array_equal(
            perturbed.vwap_eur_per_mw_h[direction][published], awards.vwap_eur_per_mw_h[direction][published]
        )
        assert not np.allclose(perturbed.awarded_mw[direction][~published], awards.awarded_mw[direction][~published])
    noisy = _perturb_market(market, cut_minute, PUBLICATION, seed=1)
    visible = PUBLICATION.visible_minute_end(cut_minute)
    for name, values in market.minute.items():
        assert np.array_equal(noisy.minute[name][:visible], values[:visible], equal_nan=True), name
    q_visible = PUBLICATION.visible_qh_end(cut_minute)
    for name, values in market.qh.items():
        assert np.array_equal(noisy.qh[name][:q_visible], values[:q_visible], equal_nan=True), name


class _PeekingStrategy(BaseStrategy):
    """Reads the current (unpublished) minute of the system imbalance: must be caught."""

    name = "peek"

    def decide(self, ctx) -> float:  # type: ignore[no-untyped-def]
        return 1.0 if ctx.market.minute["si_mw"][ctx.t] < 0 else -1.0


def test_implicit_layer_passes_the_lookahead_audit_with_activation() -> None:
    market, awards = _setup(days=2)
    activation = build_activation(market, awards)
    n = awards.blocks.n
    plan = ReservePlan.from_offers(
        "half", awards, BATTERY, offered_up_mw=np.full(n, 0.5), offered_down_mw=np.full(n, 0.5)
    )
    audit = check_reserve_no_lookahead(
        _inner, market, BATTERY, plan, activation, cut_minute=900, publication=PUBLICATION
    )
    assert audit.passed, audit
    cheat = check_reserve_no_lookahead(
        _PeekingStrategy, market, BATTERY, plan, activation, cut_minute=900, publication=PUBLICATION
    )
    assert not cheat.passed


def test_split_strategy_passes_the_core_lookahead_audit() -> None:
    market, _ = _setup(days=1)
    n_qh = market.n_qh
    up = np.where(np.arange(n_qh) % 16 < 8, 0.5, 0.0)

    def make() -> ReserveSplitStrategy:
        return ReserveSplitStrategy(_inner(), up, up[::-1].copy(), sustain_hours=0.25)

    audit = check_no_lookahead(make, market, BATTERY, cut_minute=700, publication=PUBLICATION)
    assert audit.passed, audit


def test_candidates_default_to_fixed_zero_half_full() -> None:
    assert [c.label for c in SYMMETRIC_CANDIDATES] == ["fixed_0", "fixed_50", "fixed_100"]
