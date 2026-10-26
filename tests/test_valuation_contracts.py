from __future__ import annotations

import json

import numpy as np
import pytest

from euroflex_bess_lab.valuation import (
    RiskProfile,
    ScenarioSet,
    bachelier_call,
    bachelier_put,
    black_put,
    certainty_equivalent,
    cvar,
    discount_factors,
    normal_cvar,
    normal_exponential_ce,
    price_floor,
    price_partial_toll,
    price_revenue_swap,
    price_toll,
    solve_floor_upside_share,
    toll_sensitivity,
)
from euroflex_bess_lab.valuation.contracts import tail_weights
from euroflex_bess_lab.valuation.report import render_contract, to_jsonable

NEUTRAL0 = RiskProfile(name="neutral", measure="neutral", discount_rate=0.0)
MU, SIGMA = 100_000.0, 25_000.0


def normal_paths(n_paths: int = 400_000, n_years: int = 1, seed: int = 7) -> ScenarioSet:
    rng = np.random.default_rng(seed)
    return ScenarioSet.from_annual(rng.normal(MU, SIGMA, size=(n_paths, n_years)))


def test_cvar_is_exact_on_the_empirical_distribution_and_matches_the_normal_closed_form() -> None:
    values = np.arange(1.0, 101.0)
    assert cvar(values, 0.05) == pytest.approx(3.0)
    assert cvar(values, 0.055) == pytest.approx((15.0 + 0.5 * 6.0) / 5.5)
    assert cvar(values, 1.0) == pytest.approx(values.mean())
    weights = tail_weights(values, 0.055)
    assert weights.sum() == pytest.approx(1.0) and weights @ values == pytest.approx(cvar(values, 0.055))
    sample = np.random.default_rng(1).standard_normal(400_000)
    assert cvar(sample, 0.05) == pytest.approx(normal_cvar(0.0, 1.0, 0.05), rel=0.01)


def test_certainty_equivalents_match_closed_forms_and_are_cash_invariant() -> None:
    sample = np.random.default_rng(2).normal(MU, 20_000.0, 400_000)
    cara = RiskProfile(measure="exponential", risk_tolerance_eur=100_000.0)
    assert certainty_equivalent(sample, cara) == pytest.approx(
        normal_exponential_ce(MU, 20_000.0, 100_000.0), abs=150.0
    )
    mean_cvar = RiskProfile(measure="cvar", cvar_alpha=0.1, cvar_weight=0.5)
    expected = 0.5 * MU + 0.5 * normal_cvar(MU, 20_000.0, 0.1)
    assert certainty_equivalent(sample, mean_cvar) == pytest.approx(expected, rel=0.002)
    for profile in (NEUTRAL0, cara, mean_cvar):
        shifted = certainty_equivalent(sample + 1234.5, profile)
        assert shifted == pytest.approx(certainty_equivalent(sample, profile) + 1234.5, abs=1e-6)
    # a bigger deal under CARA is riskier per MW
    assert certainty_equivalent(sample, cara, scale=5.0) < certainty_equivalent(sample, cara, scale=1.0)
    with pytest.raises(ValueError):
        RiskProfile(measure="exponential")


def test_discount_factors_use_the_mid_year_convention() -> None:
    assert discount_factors(2, 0.1) == pytest.approx([1.1**-0.5, 1.1**-1.5])
    assert discount_factors(2, 0.1, mid_year=False) == pytest.approx([1.1**-1, 1.1**-2])


def test_risk_neutral_toll_equals_mean_revenue_and_risk_aversion_lowers_the_quote() -> None:
    scenarios = ScenarioSet.from_annual(np.random.default_rng(3).normal(MU, SIGMA, size=(20_000, 5)))
    neutral = price_toll(scenarios, offtaker=NEUTRAL0, owner=NEUTRAL0)
    assert neutral.price_eur_per_mw_year == pytest.approx(scenarios.annual.mean(), rel=1e-12)
    assert neutral.break_even_eur_per_mw_year == pytest.approx(scenarios.annual.mean(), rel=1e-12)
    assert neutral.deal_zone_eur_per_mw_year == pytest.approx((neutral.price_eur_per_mw_year,) * 2)
    assert neutral.prob_offtaker_loss == pytest.approx(0.5, abs=0.02)
    assert neutral.price_eur_per_kw_month == pytest.approx(neutral.price_eur_per_mw_year / 12_000.0)

    offtaker = RiskProfile(name="offtaker", measure="cvar", cvar_weight=0.3, discount_rate=0.08)
    owner = RiskProfile(name="owner", measure="cvar", cvar_weight=0.6, discount_rate=0.08)
    averse = price_toll(scenarios, offtaker=offtaker, owner=owner)
    assert averse.price_eur_per_mw_year < averse.break_even_eur_per_mw_year
    assert averse.owner_reservation_eur_per_mw_year < averse.offtaker_reservation_eur_per_mw_year
    low, high = averse.deal_zone_eur_per_mw_year
    assert (low, high) == (averse.owner_reservation_eur_per_mw_year, averse.offtaker_reservation_eur_per_mw_year)
    assert averse.offtaker.certainty_equivalent_pv_eur_per_mw == pytest.approx(0.0, abs=1e-6)
    assert averse.expected_offtaker_margin_eur_per_mw_year == pytest.approx(
        averse.break_even_eur_per_mw_year - averse.price_eur_per_mw_year, rel=1e-9
    )
    assert averse.owner.risk_premium_eur_per_mw == pytest.approx(0.0, abs=1e-6)
    margin = RiskProfile(name="offtaker", measure="cvar", cvar_weight=0.3, required_margin_eur_per_mw_year=5000.0)
    assert price_toll(scenarios, offtaker=margin, owner=owner).price_eur_per_mw_year == pytest.approx(
        averse.price_eur_per_mw_year - 5000.0, rel=1e-9
    )
    quoted = price_toll(scenarios, offtaker=offtaker, owner=owner, toll_eur_per_mw_year=200_000.0)
    assert not quoted.priced and quoted.prob_offtaker_loss == 1.0
    json.dumps(to_jsonable(averse.to_dict()))
    assert "deal zone" in render_contract(averse)


def test_escalated_toll_has_the_same_present_value_and_cara_size_effect() -> None:
    scenarios = ScenarioSet.from_annual(np.random.default_rng(4).normal(MU, SIGMA, size=(5000, 6)))
    rate = RiskProfile(measure="neutral", discount_rate=0.07)
    flat = price_toll(scenarios, offtaker=rate, owner=rate)
    rising = price_toll(scenarios, offtaker=rate, owner=rate, escalation_per_year=0.03)
    factors = discount_factors(6, 0.07)
    growth = 1.03 ** np.arange(6)
    assert rising.price_eur_per_mw_year * float(factors @ growth) == pytest.approx(
        flat.price_eur_per_mw_year * float(factors.sum()), rel=1e-12
    )
    cara = RiskProfile(measure="exponential", risk_tolerance_eur=2_000_000.0)
    small = price_toll(scenarios, offtaker=cara, owner=rate, deal_mw=1.0)
    large = price_toll(scenarios, offtaker=cara, owner=rate, deal_mw=50.0)
    assert large.price_eur_per_mw_year < small.price_eur_per_mw_year < flat.price_eur_per_mw_year


def test_floor_premium_matches_bachelier_and_black_closed_forms() -> None:
    normal = normal_paths()
    for strike in (60_000.0, 80_000.0, 100_000.0):
        result = price_floor(normal, floor_eur_per_mw_year=strike, offtaker=NEUTRAL0, owner=NEUTRAL0)
        assert result.price_eur_per_mw_year == pytest.approx(bachelier_put(MU, strike, SIGMA), rel=0.015)
        assert result.extras["prob_floor_triggered_year"] == pytest.approx(
            float(np.mean(normal.annual < strike)), abs=1e-12
        )
    rng = np.random.default_rng(8)
    s = 0.3
    lognormal = ScenarioSet.from_annual((MU * np.exp(s * rng.standard_normal(400_000) - 0.5 * s * s))[:, None])
    result = price_floor(lognormal, floor_eur_per_mw_year=80_000.0, offtaker=NEUTRAL0, owner=NEUTRAL0)
    assert result.price_eur_per_mw_year == pytest.approx(black_put(MU, 80_000.0, s), rel=0.015)
    shared = price_floor(
        normal,
        floor_eur_per_mw_year=80_000.0,
        upside_share=0.2,
        upside_threshold_eur_per_mw_year=120_000.0,
        offtaker=NEUTRAL0,
        owner=NEUTRAL0,
    )
    expected = bachelier_put(MU, 80_000.0, SIGMA) - 0.2 * bachelier_call(MU, 120_000.0, SIGMA)
    assert shared.price_eur_per_mw_year == pytest.approx(expected, rel=0.03)


def test_floor_counterparty_views_and_zero_premium_upside_share() -> None:
    scenarios = normal_paths(100_000, n_years=3)
    provider = RiskProfile(name="provider", measure="neutral", discount_rate=0.0)
    owner = RiskProfile(name="owner", measure="cvar", cvar_weight=0.7, discount_rate=0.0)
    result = price_floor(scenarios, floor_eur_per_mw_year=80_000.0, offtaker=provider, owner=owner)
    assert result.price_direction == "owner_pays"
    assert result.owner_reservation_eur_per_mw_year > result.offtaker_reservation_eur_per_mw_year
    assert result.deal_zone_eur_per_mw_year is not None
    assert result.owner.risk_adjusted_gain_eur_per_mw_year > 0.0

    share = solve_floor_upside_share(
        scenarios, floor_eur_per_mw_year=80_000.0, upside_threshold_eur_per_mw_year=110_000.0, offtaker=provider
    )
    assert share is not None
    revenue = scenarios.annual
    closed = np.maximum(80_000.0 - revenue, 0.0).mean() / np.maximum(revenue - 110_000.0, 0.0).mean()
    assert share == pytest.approx(closed, abs=1e-5)
    collar = price_floor(
        scenarios,
        floor_eur_per_mw_year=80_000.0,
        upside_share=share,
        upside_threshold_eur_per_mw_year=110_000.0,
        offtaker=provider,
    )
    assert abs(collar.price_eur_per_mw_year) < 1.0
    assert solve_floor_upside_share(scenarios, floor_eur_per_mw_year=200_000.0, offtaker=provider) is None


def test_partial_toll_limits_and_risk_neutral_closed_form() -> None:
    scenarios = ScenarioSet.from_annual(np.random.default_rng(9).normal(MU, SIGMA, size=(20_000, 4)))
    offtaker = RiskProfile(name="offtaker", measure="cvar", cvar_weight=0.3)
    owner = RiskProfile(name="owner", measure="cvar", cvar_weight=0.6)
    full = price_toll(scenarios, offtaker=offtaker, owner=owner)
    same = price_partial_toll(scenarios, fixed_fraction=1.0, owner_merchant_share=0.3, offtaker=offtaker, owner=owner)
    assert same.price_eur_per_mw_year == pytest.approx(full.price_eur_per_mw_year, rel=1e-12)
    assert same.owner_reservation_eur_per_mw_year == pytest.approx(full.owner_reservation_eur_per_mw_year, rel=1e-12)
    half = price_partial_toll(
        scenarios, fixed_fraction=0.5, owner_merchant_share=0.8, offtaker=NEUTRAL0, owner=NEUTRAL0
    )
    assert half.price_eur_per_mw_year == pytest.approx(1.2 * scenarios.annual.mean(), rel=1e-12)
    assert half.owner_reservation_eur_per_mw_year == pytest.approx(1.2 * scenarios.annual.mean(), rel=1e-12)
    owner_cash = half.owner.expected_annual_cash_eur_per_mw_year
    assert owner_cash == pytest.approx(0.5 * half.price_eur_per_mw_year + 0.4 * scenarios.annual.mean(), rel=1e-9)
    with pytest.raises(ValueError):
        price_partial_toll(scenarios, fixed_fraction=0.0, owner_merchant_share=0.5)


def test_swap_without_basis_is_a_financial_toll_and_basis_reduces_hedge_effectiveness() -> None:
    rng = np.random.default_rng(10)
    asset = rng.normal(MU, SIGMA, size=(20_000, 5))
    index = 0.8 * asset + rng.normal(0.0, 5_000.0, size=asset.shape)
    scenarios = ScenarioSet.from_annual({"asset": asset, "index": index})
    offtaker = RiskProfile(name="offtaker", measure="cvar", cvar_weight=0.3)
    owner = RiskProfile(name="owner", measure="cvar", cvar_weight=0.6)
    toll = price_toll(scenarios, offtaker=offtaker, owner=owner)
    swap = price_revenue_swap(scenarios, offtaker=offtaker, owner=owner)
    assert swap.price_eur_per_mw_year == pytest.approx(toll.price_eur_per_mw_year, rel=1e-12)
    assert swap.owner_reservation_eur_per_mw_year == pytest.approx(toll.owner_reservation_eur_per_mw_year, rel=1e-12)
    assert swap.extras["hedge_effectiveness_pv_variance"] == pytest.approx(1.0)
    basis = price_revenue_swap(
        scenarios, reference_series="index", asset_series="asset", offtaker=offtaker, owner=owner
    )
    assert 0.5 < basis.extras["hedge_effectiveness_pv_variance"] < 1.0
    assert basis.extras["annual_correlation_reference_asset"] > 0.9
    assert basis.price_eur_per_mw_year < swap.price_eur_per_mw_year


def test_toll_sensitivity_is_monotone() -> None:
    scenarios = ScenarioSet.from_annual(np.random.default_rng(11).normal(MU, SIGMA, size=(5000, 3)))
    table = toll_sensitivity(scenarios, [60_000.0, 80_000.0, 100_000.0, 120_000.0])
    assert np.all(np.diff(table["expected_margin_eur_per_mw_year"]) < 0.0)
    assert np.all(np.diff(table["prob_offtaker_loss"]) >= 0.0)
    assert np.all(table["risk_adjusted_margin_eur_per_mw_year"] <= table["expected_margin_eur_per_mw_year"])
