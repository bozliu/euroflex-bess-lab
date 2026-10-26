"""Portfolio aggregation of several assets / markets from their daily P&L.

Assets are aligned on common complete local days (Belgium and Germany share the CET delivery day). Risk is
reported on two horizons:

- **daily**: empirical distribution of daily P&L (EUR);
- **annual**: one calendar year of jointly block-bootstrapped days (the same history days for every asset on each
  path, so cross-asset correlation, seasonality and volatility clusters are preserved).

``VaR_a`` is the ``a``-quantile of P&L and ``CVaR_a`` the mean of the worst ``a`` share (both P&L levels, not
losses). The *shortfall* ``E[X] - CVaR_a(X)`` is subadditive, so the diversification benefit
``sum_i shortfall_i - shortfall_portfolio`` is non-negative. Euler (component) contributions of every asset to
the portfolio CVaR sum exactly to the portfolio figure.
"""

from __future__ import annotations

import math
from collections.abc import Sequence
from dataclasses import dataclass
from typing import Any

import numpy as np
import pandas as pd
from numpy.typing import NDArray

from .contracts import cvar, tail_weights
from .revenue import DAYS_PER_YEAR, _jsonable
from .scenarios import DEFAULT_SEED, BootstrapConfig, _HistorySampler, daily_panel

FloatArray = NDArray[np.float64]


@dataclass(frozen=True)
class PortfolioAsset:
    """One asset's daily P&L table (from :func:`~euroflex_bess_lab.valuation.revenue.daily_pnl`).

    ``weight`` scales the ledger's EUR P&L (e.g. contracted MW / simulated MW).
    """

    name: str
    daily: pd.DataFrame
    weight: float = 1.0
    market: str | None = None
    power_mw: float | None = None


@dataclass(frozen=True)
class PortfolioResult:
    assets: tuple[str, ...]
    first_day: str
    last_day: str
    n_days: int
    alpha: float
    value_column: str
    correlation: pd.DataFrame
    rank_correlation: pd.DataFrame
    stats: pd.DataFrame
    contributions: pd.DataFrame
    diversification: dict[str, float]
    concentration: dict[str, float | None]
    annual: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return _jsonable(
            {
                "units": "EUR (weighted asset P&L)",
                "assets": list(self.assets),
                "first_day": self.first_day,
                "last_day": self.last_day,
                "n_days": self.n_days,
                "alpha": self.alpha,
                "value_column": self.value_column,
                "correlation": self.correlation.to_dict(),
                "rank_correlation": self.rank_correlation.to_dict(),
                "stats": self.stats.to_dict(orient="records"),
                "contributions": self.contributions.to_dict(orient="records"),
                "diversification": self.diversification,
                "concentration": self.concentration,
                "annual": self.annual,
            }
        )


def _risk_row(name: str, horizon: str, values: FloatArray, alpha: float) -> dict[str, Any]:
    mean = float(values.mean())
    tail = cvar(values, alpha)
    return {
        "name": name,
        "horizon": horizon,
        "mean_eur": mean,
        "std_eur": float(values.std(ddof=1)) if len(values) > 1 else 0.0,
        "var_eur": float(np.quantile(values, alpha)),
        "cvar_eur": tail,
        "shortfall_eur": mean - tail,
        "prob_negative": float(np.mean(values < 0.0)),
    }


def _hhi(shares: FloatArray) -> float | None:
    positive = np.clip(shares, 0.0, None)
    total = positive.sum()
    if total <= 0.0:
        return None
    normalised = positive / total
    return float(np.sum(normalised**2))


def _euler(matrix: FloatArray, alpha: float) -> tuple[FloatArray, FloatArray]:
    """Component CVaR and component shortfall of each column with respect to the row sum."""
    total = matrix.sum(axis=1)
    weights = tail_weights(total, alpha)
    component_cvar = weights @ matrix
    component_shortfall = matrix.mean(axis=0) - component_cvar
    return component_cvar, component_shortfall


def portfolio_risk(
    assets: Sequence[PortfolioAsset],
    *,
    value_column: str = "pnl_eur",
    alpha: float = 0.05,
    min_coverage: float = 0.98,
    annual_paths: int = 2000,
    annual_year: int = 2027,
    seed: int = DEFAULT_SEED,
    bootstrap: BootstrapConfig | None = None,
) -> PortfolioResult:
    """Correlation, VaR / CVaR, diversification benefit, Euler contributions and concentration of a portfolio."""
    if not assets:
        raise ValueError("portfolio needs at least one asset")
    names = [asset.name for asset in assets]
    if len(set(names)) != len(names):
        raise ValueError("asset names must be unique")
    if not 0.0 < alpha < 1.0:
        raise ValueError("alpha must be in (0, 1)")
    panel = daily_panel({a.name: a.daily for a in assets}, value_column=value_column, min_coverage=min_coverage)
    panel = panel.loc[:, names] * np.asarray([a.weight for a in assets], dtype=float)
    matrix = panel.to_numpy(dtype=float)
    total = matrix.sum(axis=1)
    n_days = len(panel)

    correlation = panel.corr(method="pearson") if len(names) > 1 else pd.DataFrame([[1.0]], index=names, columns=names)
    rank_correlation = (
        panel.rank().corr(method="pearson") if len(names) > 1 else pd.DataFrame([[1.0]], index=names, columns=names)
    )

    rows = [_risk_row(name, "daily", matrix[:, i], alpha) for i, name in enumerate(names)]
    rows.append(_risk_row("portfolio", "daily", total, alpha))
    for row in rows:
        row["annualised_mean_eur"] = row["mean_eur"] * DAYS_PER_YEAR
        row["annual_std_iid_eur"] = row["std_eur"] * math.sqrt(DAYS_PER_YEAR)

    sampler = _HistorySampler(panel, bootstrap or BootstrapConfig())
    calendar = pd.date_range(f"{annual_year}-01-01", f"{annual_year}-12-31", freq="D")
    annual, diagnostics = sampler.simulate(
        calendar,
        np.zeros(len(calendar), dtype=np.int64),
        1,
        annual_paths,
        np.random.default_rng(np.random.SeedSequence(seed)),
    )
    annual_matrix = annual[:, 0, :]
    annual_total = annual_matrix.sum(axis=1)
    annual_rows = [_risk_row(name, "annual", annual_matrix[:, i], alpha) for i, name in enumerate(names)]
    annual_rows.append(_risk_row("portfolio", "annual", annual_total, alpha))
    stats = pd.DataFrame(rows + annual_rows)

    def benefit(horizon: str, column: str) -> tuple[float, float]:
        frame = stats.loc[stats["horizon"] == horizon]
        standalone = float(frame.loc[frame["name"] != "portfolio", column].sum())
        combined = float(frame.loc[frame["name"] == "portfolio", column].iloc[0])
        gain = standalone - combined
        return gain, gain / standalone if standalone > 0.0 else float("nan")

    diversification: dict[str, float] = {}
    for horizon in ("daily", "annual"):
        for column, label in (("shortfall_eur", "shortfall"), ("std_eur", "std")):
            gain, pct = benefit(horizon, column)
            diversification[f"{horizon}_{label}_benefit_eur"] = gain
            diversification[f"{horizon}_{label}_benefit_pct"] = pct

    daily_cvar, daily_shortfall = _euler(matrix, alpha)
    annual_cvar, annual_shortfall = _euler(annual_matrix, alpha)
    variance = float(np.var(total))
    covariance = np.asarray([np.cov(matrix[:, i], total, ddof=0)[0, 1] for i in range(len(names))])
    means = matrix.mean(axis=0)
    portfolio_shortfall = float(annual_total.mean() - cvar(annual_total, alpha))
    contributions = pd.DataFrame(
        {
            "name": names,
            "weight": [a.weight for a in assets],
            "market": [a.market for a in assets],
            "expected_daily_eur": means,
            "expected_share": means / means.sum() if means.sum() != 0.0 else np.nan,
            "variance_share": covariance / variance if variance > 0.0 else np.nan,
            "daily_component_cvar_eur": daily_cvar,
            "daily_component_shortfall_eur": daily_shortfall,
            "annual_component_cvar_eur": annual_cvar,
            "annual_component_shortfall_eur": annual_shortfall,
            "annual_shortfall_share": annual_shortfall / portfolio_shortfall if portfolio_shortfall else np.nan,
        }
    )

    hhi_expected = _hhi(means)
    hhi_risk = _hhi(annual_shortfall)
    powers = [a.power_mw for a in assets]
    hhi_mw = _hhi(np.asarray(powers, dtype=float) * np.asarray([a.weight for a in assets])) if all(powers) else None
    descending = np.sort(total)[::-1]
    n_top = max(1, int(math.ceil(0.1 * n_days)))
    concentration: dict[str, float | None] = {
        "hhi_expected_pnl": hhi_expected,
        "effective_assets_expected_pnl": 1.0 / hhi_expected if hhi_expected else None,
        "largest_expected_share": float(np.max(np.clip(means, 0.0, None)) / np.clip(means, 0.0, None).sum())
        if np.clip(means, 0.0, None).sum() > 0.0
        else None,
        "hhi_annual_shortfall": hhi_risk,
        "effective_assets_risk": 1.0 / hhi_risk if hhi_risk else None,
        "hhi_mw": hhi_mw,
        "top_decile_day_share": float(descending[:n_top].sum() / total.sum()) if total.sum() > 0.0 else None,
    }
    index = pd.DatetimeIndex(panel.index)
    return PortfolioResult(
        assets=tuple(names),
        first_day=index[0].date().isoformat(),
        last_day=index[-1].date().isoformat(),
        n_days=n_days,
        alpha=alpha,
        value_column=value_column,
        correlation=correlation,
        rank_correlation=rank_correlation,
        stats=stats,
        contributions=contributions,
        diversification=diversification,
        concentration=concentration,
        annual={
            "year": annual_year,
            "paths": annual_paths,
            "seed": seed,
            "bootstrap": (bootstrap or BootstrapConfig()).to_dict(),
            "diagnostics": diagnostics,
        },
    )
