"""Causality of every GB forecast input: values published after the decision time never change a forecast."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pytest

from euroflex_bess_lab.markets.gb import GBMarket
from euroflex_bess_lab.markets.gb.forecast import (
    ForecastSettings,
    PriceForecaster,
    eac_persistence_forecast,
    expected_response_energy,
    last_usable_index,
    replacement_prices,
)
from euroflex_bess_lab.markets.gb.rules import RESPONSE_PRODUCTS
from euroflex_bess_lab.markets.gb.strategies import MAIN_FORECAST

REPO_ROOT = Path(__file__).resolve().parents[1]
PUBLIC = REPO_ROOT / "tests" / "fixtures" / "gb" / "public"


@pytest.fixture()
def market() -> GBMarket:
    return GBMarket.from_public("2025-06-03", "2025-06-13", root=PUBLIC)


def test_publication_lags() -> None:
    assert last_usable_index(100, 0) == 99  # MID of SP k usable from the end of SP k
    assert last_usable_index(100, 30) == 98  # system price usable 30 min after the SP end


@pytest.mark.parametrize("t", [150, 233, 301])
@pytest.mark.parametrize("settings", [MAIN_FORECAST, ForecastSettings(level_decay_per_sp=0.0, profile_days=1)])
def test_price_forecasts_ignore_unpublished_values(market: GBMarket, t: int, settings: ForecastSettings) -> None:
    targets = np.arange(t, t + 60)
    base = PriceForecaster(market, settings)
    mid_fc, sip_fc = base.mid_forecast(t, targets), base.sip_forecast(t, targets)
    shocked = GBMarket(**{**market.__dict__})
    shocked.mid = market.mid.copy()
    shocked.sip = market.sip.copy()
    shocked.mid[t:] = 9_999.0  # MID of SP t is not yet published at the start of SP t
    shocked.sip[t - 1 :] = -9_999.0  # nor is the system price of SP t - 1
    test = PriceForecaster(shocked, settings)
    assert np.allclose(test.mid_forecast(t, targets), mid_fc)
    assert np.allclose(test.sip_forecast(t, targets), sip_fc)
    # one more published SP does change the forecast (the shock is visible from the next decision on)
    assert not np.allclose(test.mid_forecast(t + 1, targets + 1), base.mid_forecast(t + 1, targets + 1))


def test_response_inputs_ignore_unpublished_values(market: GBMarket) -> None:
    t = int(market.eac_gate_sps()[5])
    day = int(market.efa_day[t + 18])
    prices = eac_persistence_forecast(market, day)
    energy = expected_response_energy(market, t)
    repl, mean = replacement_prices(market, t)
    shocked = GBMarket(**{**market.__dict__})
    shocked.eac_price = {p: market.eac_price[p].copy() for p in RESPONSE_PRODUCTS}
    shocked.response_energy = {p: market.response_energy[p].copy() for p in RESPONSE_PRODUCTS}
    shocked.sip = market.sip.copy()
    for p in RESPONSE_PRODUCTS:
        shocked.eac_price[p][market.efa_day >= day] = 777.0  # target day and later: published at/after the gate
        shocked.response_energy[p][t:] = 0.4
    shocked.sip[t - 1 :] = 5_000.0
    assert eac_persistence_forecast(shocked, day) == prices
    assert all(np.allclose(expected_response_energy(shocked, t)[p], energy[p]) for p in RESPONSE_PRODUCTS)
    repl2, mean2 = replacement_prices(shocked, t)
    assert repl2 == pytest.approx(repl) and mean2 == pytest.approx(mean)
    # the previous EFA day (published 15:00 two days before the target) is what persistence uses
    prev = market.block_sps(day - 1, 3)[0]
    assert prices[2]["DCL"] == pytest.approx(float(market.eac_price["DCL"][prev]))


def test_replacement_prices_reflect_adverse_timing(market: GBMarket) -> None:
    t = market.n - 1
    repl, mean = replacement_prices(market, t, days=9)
    # exports (low products) are replaced when the system tends to be short, imports sold when it tends to be long
    assert repl["DRL"] > repl["DRH"]
    assert np.isfinite(mean)
