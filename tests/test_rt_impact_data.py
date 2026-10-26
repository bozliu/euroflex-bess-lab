"""Impact operators on real Elia data: the committed fixture slice (fast) and the full data set (slow)."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from euroflex_bess_lab.realtime import BatteryParams, MarketData, check_no_lookahead
from euroflex_bess_lab.realtime.impact import (
    CurrentFormulaImpact,
    NewFormula2026Impact,
    current_state_from_market,
    fit_current_response,
    fit_new_formula_response,
)
from euroflex_bess_lab.realtime.impact.policies import ImpactAwareThresholdStrategy, VolumeFromSI
from euroflex_bess_lab.realtime.impact.validation import validate_operator_si_shift

REPO = Path(__file__).resolve().parents[1]
FIXTURE = REPO / "tests" / "fixtures" / "public"
BATTERY = BatteryParams(power_mw=1.0, energy_mwh=2.0)
Q_GRID = np.linspace(-1200.0, 1200.0, 121)


@pytest.fixture(scope="module")
def fixture_market():
    if not (FIXTURE / "belgium" / "new_formula_qh_2026.parquet").exists():
        pytest.skip("public-data fixture slice not available")
    mp = pytest.MonkeyPatch()
    mp.setenv("EUROFLEX_DATA_DIR", str(FIXTURE))
    try:
        market = MarketData.from_public(
            "2026-03-02", "2026-03-05", include_new_formula=True, include_si_forecast=False, include_day_ahead=False
        )
    finally:
        mp.undo()
    return market


def _branch(op, si2: np.ndarray) -> np.ndarray:
    """Branch id: short / long, plus the neutral |SI'| < 25 MW band of the current rule (when FLOOR/CAP exist)."""
    branch = np.where(si2 < 0.0, -1, 1)
    if isinstance(op, CurrentFormulaImpact):
        has_mid = np.isfinite(op.state.floor_eur_mwh + op.state.cap_eur_mwh)[:, None]
        branch = np.where((np.abs(si2) < 25.0) & has_mid, 0, branch)
    return branch


def _assert_monotone(op) -> None:
    prices = op.price_at(np.tile(Q_GRID, (len(op), 1)))
    si2 = op.state.si_mw[:, None] + Q_GRID[None, :]
    branch = _branch(op, si2)
    same = branch[:, 1:] == branch[:, :-1]
    steps = np.diff(prices, axis=1)
    assert np.all(steps[same & np.isfinite(steps)] <= 1e-9)


def test_operators_reproduce_published_prices_on_the_fixture(fixture_market) -> None:
    m = fixture_market
    fit_c, alpha_fit = fit_current_response(m, min_regime_obs=50)
    fit_n = fit_new_formula_response(m, min_regime_obs=50)
    cur = CurrentFormulaImpact.from_market(m, fit_c)
    new = NewFormula2026Impact.from_market(m, fit_n)
    for op, mech in ((cur, "current"), (new, "new2026")):
        rec, pub = op.reconstruct(), m.settlement(mech)
        ok = np.isfinite(rec) & np.isfinite(pub)
        assert ok.mean() > 0.95
        assert np.mean(np.isclose(rec[ok], pub[ok], atol=0.011)) > 0.99
        _assert_monotone(op)
        idx = np.arange(m.n_qh, dtype=np.int64)
        assert np.array_equal(op.adjust(idx, pub, np.zeros(m.n_qh)), pub, equal_nan=True)
    assert 0.0 <= fit_c.pass_through <= 1.0 and 0.0 <= fit_n.pass_through <= 1.0
    assert alpha_fit.zero_rule_share == 1.0
    assert alpha_fit.exact_rule_share > 0.99


def test_sign_flips_in_real_quarter_hours_switch_the_branch(fixture_market) -> None:
    m = fixture_market
    fit_c, _ = fit_current_response(m, min_regime_obs=50)
    op = CurrentFormulaImpact.from_market(m, fit_c)
    si = op.state.si_mw
    k = int(np.flatnonzero(si < -200.0)[0])
    q = np.zeros(m.n_qh)
    q[k] = abs(si[k]) + 300.0
    price = op.adjust(np.arange(m.n_qh, dtype=np.int64), m.settlement("current"), q)
    # long branch after the flip: at most MDP (minus alpha), far below the published short price
    assert price[k] <= op.state.mdp_eur_mwh[k] + 1e-6
    assert price[k] < m.settlement("current")[k]


def test_impact_aware_policy_on_real_data_is_causal(fixture_market) -> None:
    m = fixture_market
    fit_n = fit_new_formula_response(m, min_regime_obs=50)
    audit = check_no_lookahead(
        lambda: ImpactAwareThresholdStrategy("new2026", fit_n, 300.0, min_history_qh=20),
        m,
        BATTERY,
        cut_minute=2000,
        mechanism="new2026",
    )
    assert audit.passed, audit
    fit_c, _ = fit_current_response(m, min_regime_obs=50)
    st = current_state_from_market(m)
    vmap = VolumeFromSI.fit(st.si_mw, st.up_mw, st.down_mw)
    audit_c = check_no_lookahead(
        lambda: ImpactAwareThresholdStrategy("current", fit_c, 300.0, volume_map=vmap, min_history_qh=20),
        m,
        BATTERY,
        cut_minute=2000,
        mechanism="current",
    )
    assert audit_c.passed, audit_c


@pytest.mark.slow
def test_full_data_validation_headlines() -> None:
    market = MarketData.from_public(
        "2025-07-01", "2026-08-01", include_new_formula=False, include_si_forecast=False, include_day_ahead=False
    )
    train, test = market.slice("2025-07-01", "2026-01-01"), market.slice("2026-01-01", "2026-08-01")
    fit, alpha_fit = fit_current_response(train)
    assert alpha_fit.model.feature == "mean_abs_si_prev"
    assert alpha_fit.exact_rule_share > 0.999  # Elia's alpha rule incl. the price factor
    assert alpha_fit.tariff_exact_share > 0.6
    op = CurrentFormulaImpact.from_market(test, fit)
    shift = validate_operator_si_shift("current", op, test.settlement("current"))
    flips = shift["sign_flip"]["vs_persistence"]
    assert flips["mae"] < flips["baseline_mae"]
    _assert_monotone(CurrentFormulaImpact.from_market(test.slice("2026-03-02", "2026-03-09"), fit))


@pytest.mark.slow
def test_capacity_value_curve_ledger_contract() -> None:
    path = REPO / "reports" / "ledgers" / "impact_capacity_value_curve.parquet"
    if not path.exists():
        pytest.skip("run scripts/research/impact_study.py first")
    frame = pd.read_parquet(path)
    required = ["mechanism", "policy", "fleet_mw", "eur_per_mw_year", "cycles_per_day", "overshoot_share"]
    assert list(frame.columns[: len(required)]) == required
    assert set(frame["mechanism"]) == {"current", "new2026"}
    assert {1.0, 1600.0} <= set(frame["fleet_mw"])
    assert (REPO / "reports" / "ledgers" / "impact_capacity_value_curve.json").exists()
