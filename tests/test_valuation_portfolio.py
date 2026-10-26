from __future__ import annotations

import json

import numpy as np
import pandas as pd
import pytest

from euroflex_bess_lab.valuation import PortfolioAsset, cvar, portfolio_risk
from euroflex_bess_lab.valuation.plots import plot_portfolio_risk
from euroflex_bess_lab.valuation.report import render_portfolio, to_jsonable

DATES = pd.date_range("2025-01-01", periods=730, freq="D")


def daily(values: np.ndarray) -> pd.DataFrame:
    return pd.DataFrame({"date": DATES, "pnl_eur": values, "coverage": 1.0})


def test_perfectly_correlated_assets_have_no_diversification_benefit() -> None:
    values = 500.0 + np.random.default_rng(1).normal(0.0, 200.0, len(DATES))
    result = portfolio_risk([PortfolioAsset("a", daily(values)), PortfolioAsset("b", daily(values))], annual_paths=300)
    assert result.correlation.loc["a", "b"] == pytest.approx(1.0)
    assert result.diversification["daily_shortfall_benefit_eur"] == pytest.approx(0.0, abs=1e-6)
    assert result.diversification["annual_shortfall_benefit_eur"] == pytest.approx(0.0, abs=1e-6)
    assert result.concentration["hhi_expected_pnl"] == pytest.approx(0.5)
    assert result.concentration["effective_assets_expected_pnl"] == pytest.approx(2.0)


def test_independent_assets_diversify_and_euler_contributions_add_up() -> None:
    rng = np.random.default_rng(2)
    be = 450.0 + rng.normal(0.0, 230.0, len(DATES))
    de = 130.0 + rng.normal(0.0, 80.0, len(DATES))
    assets = [
        PortfolioAsset("be", daily(be), weight=2.0, market="BE", power_mw=1.0),
        PortfolioAsset("de", daily(de), market="DE-LU", power_mw=1.0),
    ]
    result = portfolio_risk(assets, alpha=0.05, annual_paths=500)
    assert result.n_days == len(DATES)
    assert abs(result.correlation.loc["be", "de"]) < 0.1
    assert result.diversification["daily_shortfall_benefit_eur"] > 0.0
    assert result.diversification["annual_std_benefit_pct"] > 0.0
    total = 2.0 * be + de
    portfolio_daily = result.stats.query("name == 'portfolio' and horizon == 'daily'").iloc[0]
    assert portfolio_daily["cvar_eur"] == pytest.approx(cvar(total, 0.05))
    assert result.contributions["daily_component_cvar_eur"].sum() == pytest.approx(portfolio_daily["cvar_eur"])
    portfolio_annual = result.stats.query("name == 'portfolio' and horizon == 'annual'").iloc[0]
    assert result.contributions["annual_component_shortfall_eur"].sum() == pytest.approx(
        portfolio_annual["shortfall_eur"]
    )
    assert result.contributions["annual_shortfall_share"].sum() == pytest.approx(1.0)
    assert result.contributions["variance_share"].sum() == pytest.approx(1.0)
    assert result.contributions["expected_share"].iloc[0] == pytest.approx(2.0 * be.mean() / total.mean())
    assert result.concentration["hhi_mw"] == pytest.approx((2 / 3) ** 2 + (1 / 3) ** 2)
    json.dumps(to_jsonable(result.to_dict()))
    assert "Diversification benefit" in render_portfolio(result)


def test_portfolio_uses_common_complete_days_and_validates_inputs(tmp_path) -> None:
    partial = daily(np.full(len(DATES), 100.0))
    partial.loc[:9, "coverage"] = 0.5
    result = portfolio_risk(
        [PortfolioAsset("a", partial), PortfolioAsset("b", daily(np.full(len(DATES), 50.0)))], annual_paths=50
    )
    assert result.n_days == len(DATES) - 10
    assert result.first_day == DATES[10].date().isoformat()
    path = plot_portfolio_risk(result, tmp_path / "portfolio.png")
    assert path.exists() and path.stat().st_size > 5_000
    with pytest.raises(ValueError, match="unique"):
        portfolio_risk([PortfolioAsset("a", partial), PortfolioAsset("a", partial)])
    with pytest.raises(ValueError):
        portfolio_risk([])
