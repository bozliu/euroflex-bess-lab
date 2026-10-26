"""Contract pricing on simulated annual revenue paths: tolls, revenue floors, partial tolls and revenue swaps.

All prices are in EUR per MW (of battery power) per year, on the annual revenue paths ``R[p, y]`` of a
:class:`~euroflex_bess_lab.valuation.scenarios.ScenarioSet`. Each counterparty discounts at its own rate
(mid-year convention by default) and values a stream of cash flows by a certainty equivalent ``CE`` of its present
value across paths:

- ``neutral``: ``CE = E[PV]``;
- ``cvar``: ``CE = (1 - w) E[PV] + w CVaR_a(PV)`` with ``CVaR_a`` the mean of the worst ``a`` share of outcomes;
- ``exponential``: ``CE = -T ln E[exp(-PV / T)]`` (CARA utility with risk tolerance ``T`` EUR, applied to the whole
  deal of ``deal_mw`` MW).

All three are cash-invariant (``CE(X + c) = CE(X) + c``) and monotone, so every reservation price has a closed form
given the CE of the risky leg. The *fair* price of a contract is the offtaker's reservation price - the
optimiser's risk-adjusted quote (certainty equivalent of the merchant leg minus its required margin) - and the owner's
reservation price bounds the deal zone from the other side.
"""

from __future__ import annotations

import math
from collections.abc import Callable, Sequence
from dataclasses import asdict, dataclass, field
from statistics import NormalDist
from typing import Any, Literal

import numpy as np
import pandas as pd
from numpy.typing import ArrayLike, NDArray

from .scenarios import ScenarioSet, exceedance_levels

FloatArray = NDArray[np.float64]
RiskMeasure = Literal["neutral", "cvar", "exponential"]
PriceDirection = Literal["offtaker_pays", "owner_pays"]

_STANDARD_NORMAL = NormalDist()


# ---------------------------------------------------------------------------------------------------------------
# Risk preferences
# ---------------------------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class RiskProfile:
    """Risk preference, discount rate and required margin of one counterparty."""

    name: str = "counterparty"
    measure: RiskMeasure = "cvar"
    cvar_alpha: float = 0.10
    cvar_weight: float = 0.5
    risk_tolerance_eur: float | None = None
    discount_rate: float = 0.08
    required_margin_eur_per_mw_year: float = 0.0

    def __post_init__(self) -> None:
        if self.measure not in ("neutral", "cvar", "exponential"):
            raise ValueError("measure must be 'neutral', 'cvar' or 'exponential'")
        if not 0.0 < self.cvar_alpha <= 1.0:
            raise ValueError("cvar_alpha must be in (0, 1]")
        if not 0.0 <= self.cvar_weight <= 1.0:
            raise ValueError("cvar_weight must be in [0, 1]")
        if self.measure == "exponential" and (self.risk_tolerance_eur is None or self.risk_tolerance_eur <= 0.0):
            raise ValueError("exponential utility needs a positive risk_tolerance_eur")
        if self.discount_rate <= -1.0:
            raise ValueError("discount_rate must be > -100 %")

    def describe(self) -> str:
        if self.measure == "neutral":
            risk = "risk-neutral"
        elif self.measure == "cvar":
            risk = f"mean-CVaR (weight {self.cvar_weight:g} on CVaR {self.cvar_alpha:.0%})"
        else:
            risk = f"exponential utility (risk tolerance EUR {self.risk_tolerance_eur:,.0f})"
        margin = (
            f", required margin EUR {self.required_margin_eur_per_mw_year:,.0f}/MW/yr"
            if self.required_margin_eur_per_mw_year
            else ""
        )
        return f"{risk}, discount rate {self.discount_rate:.1%}{margin}"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def default_offtaker() -> RiskProfile:
    """Illustrative optimiser profile: diversified book, moderate tail aversion."""
    return RiskProfile(name="offtaker", measure="cvar", cvar_alpha=0.10, cvar_weight=0.3, discount_rate=0.08)


def default_owner() -> RiskProfile:
    """Illustrative owner profile: single asset with project debt, stronger tail aversion."""
    return RiskProfile(name="owner", measure="cvar", cvar_alpha=0.10, cvar_weight=0.6, discount_rate=0.08)


def value_at_risk(values: ArrayLike, alpha: float) -> float:
    """Lower ``alpha``-quantile of the outcome distribution (a P&L level, not a loss)."""
    x = np.asarray(values, dtype=float).ravel()
    return float(np.quantile(x, alpha))


def cvar(values: ArrayLike, alpha: float) -> float:
    """Expected outcome in the worst ``alpha`` share of scenarios (exact for the empirical distribution)."""
    if not 0.0 < alpha <= 1.0:
        raise ValueError("alpha must be in (0, 1]")
    x = np.sort(np.asarray(values, dtype=float).ravel())
    n = len(x)
    if n == 0:
        raise ValueError("cvar of an empty sample")
    k = alpha * n
    m = int(math.floor(k))
    total = float(x[:m].sum())
    if m < n:
        total += (k - m) * float(x[m])
    return total / k


def tail_weights(values: ArrayLike, alpha: float) -> FloatArray:
    """Scenario weights (summing to 1) of the worst ``alpha`` tail used by :func:`cvar` (for Euler allocation)."""
    x = np.asarray(values, dtype=float).ravel()
    n = len(x)
    k = alpha * n
    m = int(math.floor(k))
    order = np.argsort(x, kind="stable")
    weights = np.zeros(n, dtype=float)
    weights[order[:m]] = 1.0
    if m < n:
        weights[order[m]] = k - m
    result: FloatArray = weights / k
    return result


def certainty_equivalent(values: ArrayLike, profile: RiskProfile, *, scale: float = 1.0) -> float:
    """Certainty equivalent of per-MW outcomes; ``scale`` (deal MW) sizes the exposure for exponential utility."""
    x = np.asarray(values, dtype=float).ravel()
    if len(x) == 0:
        raise ValueError("certainty equivalent of an empty sample")
    if profile.measure == "neutral":
        return float(x.mean())
    if profile.measure == "cvar":
        return float((1.0 - profile.cvar_weight) * x.mean() + profile.cvar_weight * cvar(x, profile.cvar_alpha))
    tolerance = float(profile.risk_tolerance_eur or 0.0)
    if scale <= 0.0:
        raise ValueError("scale must be positive")
    z = -x * scale / tolerance
    top = float(z.max())
    log_mean_exp = top + math.log(float(np.mean(np.exp(z - top))))
    return -tolerance * log_mean_exp / scale


def discount_factors(n_years: int, rate: float, *, mid_year: bool = True) -> FloatArray:
    """``(1 + r) ** -(y + 0.5)`` (mid-year) or ``(1 + r) ** -(y + 1)`` (end of year) for ``y = 0 .. n - 1``."""
    t = np.arange(n_years, dtype=float) + (0.5 if mid_year else 1.0)
    result: FloatArray = (1.0 + rate) ** (-t)
    return result


def _escalation(n_years: int, rate: float) -> FloatArray:
    result: FloatArray = (1.0 + rate) ** np.arange(n_years, dtype=float)
    return result


# ---------------------------------------------------------------------------------------------------------------
# Closed-form benchmarks
# ---------------------------------------------------------------------------------------------------------------


def bachelier_put(forward: float, strike: float, sigma: float) -> float:
    """``E[max(K - X, 0)]`` for ``X ~ Normal(forward, sigma^2)`` (undiscounted Bachelier put)."""
    if sigma <= 0.0:
        return max(strike - forward, 0.0)
    d = (strike - forward) / sigma
    return (strike - forward) * _STANDARD_NORMAL.cdf(d) + sigma * _STANDARD_NORMAL.pdf(d)


def bachelier_call(forward: float, strike: float, sigma: float) -> float:
    """``E[max(X - K, 0)]`` for ``X ~ Normal(forward, sigma^2)``."""
    return bachelier_put(forward, strike, sigma) + forward - strike


def black_put(forward: float, strike: float, sigma_log: float) -> float:
    """``E[max(K - X, 0)]`` for lognormal ``X`` with mean ``forward`` and log-volatility ``sigma_log`` (Black-76)."""
    if sigma_log <= 0.0 or forward <= 0.0 or strike <= 0.0:
        return max(strike - forward, 0.0)
    d1 = (math.log(forward / strike) + 0.5 * sigma_log**2) / sigma_log
    d2 = d1 - sigma_log
    return strike * _STANDARD_NORMAL.cdf(-d2) - forward * _STANDARD_NORMAL.cdf(-d1)


def black_call(forward: float, strike: float, sigma_log: float) -> float:
    """``E[max(X - K, 0)]`` for lognormal ``X`` (Black-76, undiscounted)."""
    return black_put(forward, strike, sigma_log) + forward - strike


def normal_cvar(mu: float, sigma: float, alpha: float) -> float:
    """Lower-tail CVaR of ``Normal(mu, sigma^2)``: ``mu - sigma * pdf(z_a) / a``."""
    z = _STANDARD_NORMAL.inv_cdf(alpha)
    return mu - sigma * _STANDARD_NORMAL.pdf(z) / alpha


def normal_exponential_ce(mu: float, sigma: float, risk_tolerance: float) -> float:
    """CARA certainty equivalent of ``Normal(mu, sigma^2)``: ``mu - sigma^2 / (2 T)``."""
    return mu - sigma**2 / (2.0 * risk_tolerance)


# ---------------------------------------------------------------------------------------------------------------
# Results
# ---------------------------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class CounterpartyView:
    """One counterparty's valuation of its cash flows under a contract, against its no-deal benchmark."""

    name: str
    role: str
    risk: str
    discount_rate: float
    benchmark: str
    expected_pv_eur_per_mw: float
    certainty_equivalent_pv_eur_per_mw: float
    risk_premium_eur_per_mw: float
    cvar_pv_eur_per_mw: float
    benchmark_expected_pv_eur_per_mw: float
    benchmark_certainty_equivalent_pv_eur_per_mw: float
    expected_gain_pv_eur_per_mw: float
    risk_adjusted_gain_pv_eur_per_mw: float
    risk_adjusted_gain_eur_per_mw_year: float
    prob_worse_off: float
    expected_annual_cash_eur_per_mw_year: float
    annual_cash: dict[str, float]
    prob_negative_year: float
    by_year: list[dict[str, float]]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


@dataclass(frozen=True)
class ContractResult:
    """Price and both counterparties' views of one contract on one scenario set."""

    contract: str
    description: str
    price_name: str
    price_eur_per_mw_year: float
    priced: bool
    price_direction: PriceDirection
    break_even_eur_per_mw_year: float
    offtaker_reservation_eur_per_mw_year: float
    owner_reservation_eur_per_mw_year: float
    deal_zone_eur_per_mw_year: tuple[float, float] | None
    expected_offtaker_margin_eur_per_mw_year: float
    expected_offtaker_margin_pv_eur: float
    prob_offtaker_loss: float
    prob_offtaker_loss_year: float
    offtaker: CounterpartyView
    owner: CounterpartyView
    merchant: dict[str, Any]
    terms: dict[str, Any]
    extras: dict[str, Any]
    deal_mw: float
    years: tuple[int, ...]
    series: str
    notes: tuple[str, ...] = field(default_factory=tuple)

    @property
    def price_eur_per_kw_month(self) -> float:
        return self.price_eur_per_mw_year / 1000.0 / 12.0

    def to_dict(self) -> dict[str, Any]:
        payload = asdict(self)
        payload["price_eur_per_kw_month"] = self.price_eur_per_kw_month
        payload["years"] = list(self.years)
        payload["notes"] = list(self.notes)
        payload["deal_zone_eur_per_mw_year"] = (
            list(self.deal_zone_eur_per_mw_year) if self.deal_zone_eur_per_mw_year is not None else None
        )
        payload["percentile_convention"] = "exceedance: P90 is exceeded with 90 % probability (10th percentile)"
        return payload


def _merchant_summary(revenue: FloatArray, years: Sequence[int]) -> dict[str, Any]:
    by_year = []
    for j, year in enumerate(years):
        by_year.append({"year": int(year), "mean": float(revenue[:, j].mean()), **exceedance_levels(revenue[:, j])})
    return {
        "mean_annual_eur_per_mw_year": float(revenue.mean()),
        "annual_pooled": exceedance_levels(revenue),
        "tenor_average": exceedance_levels(revenue.mean(axis=1)),
        "by_year": by_year,
    }


def _view(
    profile: RiskProfile,
    *,
    role: str,
    cash: FloatArray,
    benchmark_cash: FloatArray | None,
    benchmark_label: str,
    years: Sequence[int],
    scale: float,
    mid_year: bool,
) -> CounterpartyView:
    n_paths, n_years = cash.shape
    factors = discount_factors(n_years, profile.discount_rate, mid_year=mid_year)
    pv = cash @ factors
    bench_pv = benchmark_cash @ factors if benchmark_cash is not None else np.zeros(n_paths)
    ce = certainty_equivalent(pv, profile, scale=scale)
    bench_ce = certainty_equivalent(bench_pv, profile, scale=scale) if benchmark_cash is not None else 0.0
    gain = pv - bench_pv
    annuity = float(factors.sum())
    by_year = [
        {"year": int(year), "mean": float(cash[:, j].mean()), **exceedance_levels(cash[:, j])}
        for j, year in enumerate(years)
    ]
    return CounterpartyView(
        name=profile.name,
        role=role,
        risk=profile.describe(),
        discount_rate=profile.discount_rate,
        benchmark=benchmark_label,
        expected_pv_eur_per_mw=float(pv.mean()),
        certainty_equivalent_pv_eur_per_mw=ce,
        risk_premium_eur_per_mw=float(pv.mean() - ce),
        cvar_pv_eur_per_mw=cvar(pv, profile.cvar_alpha),
        benchmark_expected_pv_eur_per_mw=float(bench_pv.mean()),
        benchmark_certainty_equivalent_pv_eur_per_mw=bench_ce,
        expected_gain_pv_eur_per_mw=float(gain.mean()),
        risk_adjusted_gain_pv_eur_per_mw=ce - bench_ce,
        risk_adjusted_gain_eur_per_mw_year=(ce - bench_ce) / annuity,
        prob_worse_off=float(np.mean(gain < 0.0)),
        expected_annual_cash_eur_per_mw_year=float(cash.mean()),
        annual_cash=exceedance_levels(cash),
        prob_negative_year=float(np.mean(cash < 0.0)),
        by_year=by_year,
    )


def _deal_zone(direction: PriceDirection, offtaker_res: float, owner_res: float) -> tuple[float, float] | None:
    if direction == "offtaker_pays":
        return (owner_res, offtaker_res) if owner_res <= offtaker_res else None
    return (offtaker_res, owner_res) if offtaker_res <= owner_res else None


def _result(
    *,
    contract: str,
    description: str,
    price_name: str,
    price: float,
    priced: bool,
    direction: PriceDirection,
    break_even: float,
    offtaker_res: float,
    owner_res: float,
    offtaker_cash: FloatArray,
    owner_cash: FloatArray,
    owner_benchmark: FloatArray,
    owner_benchmark_label: str,
    merchant: FloatArray,
    offtaker: RiskProfile,
    owner: RiskProfile,
    scenarios: ScenarioSet,
    series: str,
    deal_mw: float,
    mid_year: bool,
    terms: dict[str, Any],
    extras: dict[str, Any],
    notes: Sequence[str] = (),
) -> ContractResult:
    off_view = _view(
        offtaker,
        role="offtaker",
        cash=offtaker_cash,
        benchmark_cash=None,
        benchmark_label="no deal (zero cash)",
        years=scenarios.years,
        scale=deal_mw,
        mid_year=mid_year,
    )
    own_view = _view(
        owner,
        role="owner",
        cash=owner_cash,
        benchmark_cash=owner_benchmark,
        benchmark_label=owner_benchmark_label,
        years=scenarios.years,
        scale=deal_mw,
        mid_year=mid_year,
    )
    annuity = float(discount_factors(scenarios.n_years, offtaker.discount_rate, mid_year=mid_year).sum())
    return ContractResult(
        contract=contract,
        description=description,
        price_name=price_name,
        price_eur_per_mw_year=float(price),
        priced=priced,
        price_direction=direction,
        break_even_eur_per_mw_year=float(break_even),
        offtaker_reservation_eur_per_mw_year=float(offtaker_res),
        owner_reservation_eur_per_mw_year=float(owner_res),
        deal_zone_eur_per_mw_year=_deal_zone(direction, float(offtaker_res), float(owner_res)),
        expected_offtaker_margin_eur_per_mw_year=off_view.expected_pv_eur_per_mw / annuity,
        expected_offtaker_margin_pv_eur=off_view.expected_pv_eur_per_mw * deal_mw,
        prob_offtaker_loss=off_view.prob_worse_off,
        prob_offtaker_loss_year=off_view.prob_negative_year,
        offtaker=off_view,
        owner=own_view,
        merchant=_merchant_summary(merchant, scenarios.years),
        terms={**terms, "deal_mw": deal_mw, "mid_year_discounting": mid_year},
        extras=extras,
        deal_mw=deal_mw,
        years=scenarios.years,
        series=series,
        notes=tuple(notes),
    )


def _check_deal(deal_mw: float, owner_capture: float) -> None:
    if deal_mw <= 0.0:
        raise ValueError("deal_mw must be positive")
    if not 0.0 < owner_capture <= 1.5:
        raise ValueError("owner_capture must be in (0, 1.5]")


# ---------------------------------------------------------------------------------------------------------------
# Contracts
# ---------------------------------------------------------------------------------------------------------------


def price_toll(
    scenarios: ScenarioSet,
    *,
    offtaker: RiskProfile | None = None,
    owner: RiskProfile | None = None,
    toll_eur_per_mw_year: float | None = None,
    escalation_per_year: float = 0.0,
    deal_mw: float = 1.0,
    owner_capture: float = 1.0,
    series: str | None = None,
    mid_year: bool = True,
) -> ContractResult:
    """Fixed toll: the owner receives ``K (1 + e)^y`` per MW-year, the offtaker takes the merchant revenue.

    Offtaker reservation (its fair quote): ``K_off = (CE_off(PV R) - m A) / G``; owner reservation:
    ``K_own = CE_own(PV c R) / G`` with ``c = owner_capture`` (share of the simulated merchant revenue the owner would
    capture trading on its own) and ``G = sum_y D_y (1 + e)^y``. Break-even: ``E[PV R] / G``.
    """
    off = offtaker or default_offtaker()
    own = owner or default_owner()
    _check_deal(deal_mw, owner_capture)
    name = series or scenarios.primary
    revenue = scenarios.get(name)
    n_years = scenarios.n_years
    growth = _escalation(n_years, escalation_per_year)
    d_off = discount_factors(n_years, off.discount_rate, mid_year=mid_year)
    d_own = discount_factors(n_years, own.discount_rate, mid_year=mid_year)
    g_off, g_own = float(d_off @ growth), float(d_own @ growth)
    pv_off = revenue @ d_off
    owner_merchant = owner_capture * revenue
    break_even = float(pv_off.mean()) / g_off
    off_res = (
        certainty_equivalent(pv_off, off, scale=deal_mw) - off.required_margin_eur_per_mw_year * d_off.sum()
    ) / g_off
    own_res = certainty_equivalent(owner_merchant @ d_own, own, scale=deal_mw) / g_own
    toll = off_res if toll_eur_per_mw_year is None else float(toll_eur_per_mw_year)
    fixed = np.broadcast_to(toll * growth, revenue.shape)
    return _result(
        contract="toll",
        description="Fixed toll: owner receives a fixed fee, offtaker takes the merchant revenue",
        price_name="toll",
        price=toll,
        priced=toll_eur_per_mw_year is None,
        direction="offtaker_pays",
        break_even=break_even,
        offtaker_res=off_res,
        owner_res=own_res,
        offtaker_cash=revenue - fixed,
        owner_cash=np.array(fixed, dtype=float),
        owner_benchmark=owner_merchant,
        owner_benchmark_label=f"merchant ({owner_capture:.0%} capture of simulated revenue)",
        merchant=revenue,
        offtaker=off,
        owner=own,
        scenarios=scenarios,
        series=name,
        deal_mw=deal_mw,
        mid_year=mid_year,
        terms={
            "toll_eur_per_mw_year": toll,
            "escalation_per_year": escalation_per_year,
            "owner_capture": owner_capture,
        },
        extras={},
    )


def toll_sensitivity(
    scenarios: ScenarioSet,
    tolls: Sequence[float],
    *,
    offtaker: RiskProfile | None = None,
    escalation_per_year: float = 0.0,
    deal_mw: float = 1.0,
    series: str | None = None,
    mid_year: bool = True,
) -> pd.DataFrame:
    """Offtaker expected / risk-adjusted margin and loss probability for a grid of toll levels."""
    off = offtaker or default_offtaker()
    revenue = scenarios.get(series)
    n_years = scenarios.n_years
    growth = _escalation(n_years, escalation_per_year)
    factors = discount_factors(n_years, off.discount_rate, mid_year=mid_year)
    annuity, g = float(factors.sum()), float(factors @ growth)
    pv = revenue @ factors
    rows = []
    for toll in tolls:
        margin = pv - float(toll) * g
        rows.append(
            {
                "toll_eur_per_mw_year": float(toll),
                "expected_margin_eur_per_mw_year": float(margin.mean()) / annuity,
                "risk_adjusted_margin_eur_per_mw_year": certainty_equivalent(margin, off, scale=deal_mw) / annuity,
                "prob_offtaker_loss": float(np.mean(margin < 0.0)),
                "cvar_margin_pv_eur_per_mw": cvar(margin, off.cvar_alpha),
            }
        )
    return pd.DataFrame(rows)


def price_floor(
    scenarios: ScenarioSet,
    *,
    floor_eur_per_mw_year: float,
    premium_eur_per_mw_year: float | None = None,
    upside_share: float = 0.0,
    upside_threshold_eur_per_mw_year: float | None = None,
    offtaker: RiskProfile | None = None,
    owner: RiskProfile | None = None,
    deal_mw: float = 1.0,
    series: str | None = None,
    mid_year: bool = True,
) -> ContractResult:
    """Revenue floor: a put on annual revenue sold by the offtaker, optionally paid for with upside sharing.

    Each year the provider pays ``max(F - R_y, 0)`` and receives the premium ``P`` plus ``b max(R_y - H, 0)``
    (``b = upside_share``, ``H`` = threshold, default ``F``). Provider reservation (fair premium):
    ``P_off = (m A - CE_off(PV(b call - put))) / A``; owner reservation (maximum premium):
    ``P_own = (CE_own(PV(R + put - b call)) - CE_own(PV R)) / A``.
    """
    off = offtaker or default_offtaker()
    own = owner or default_owner()
    _check_deal(deal_mw, 1.0)
    if not 0.0 <= upside_share <= 1.0:
        raise ValueError("upside_share must be in [0, 1]")
    name = series or scenarios.primary
    revenue = scenarios.get(name)
    n_years = scenarios.n_years
    floor = float(floor_eur_per_mw_year)
    threshold = floor if upside_threshold_eur_per_mw_year is None else float(upside_threshold_eur_per_mw_year)
    put = np.maximum(floor - revenue, 0.0)
    call = np.maximum(revenue - threshold, 0.0)
    provider_flow = upside_share * call - put
    owner_with_floor = revenue + put - upside_share * call
    d_off = discount_factors(n_years, off.discount_rate, mid_year=mid_year)
    d_own = discount_factors(n_years, own.discount_rate, mid_year=mid_year)
    a_off, a_own = float(d_off.sum()), float(d_own.sum())
    break_even = float(((put - upside_share * call) @ d_off).mean()) / a_off
    off_res = (
        off.required_margin_eur_per_mw_year * a_off - certainty_equivalent(provider_flow @ d_off, off, scale=deal_mw)
    ) / a_off
    own_res = (
        certainty_equivalent(owner_with_floor @ d_own, own, scale=deal_mw)
        - certainty_equivalent(revenue @ d_own, own, scale=deal_mw)
    ) / a_own
    premium = off_res if premium_eur_per_mw_year is None else float(premium_eur_per_mw_year)
    return _result(
        contract="floor",
        description="Revenue floor: owner keeps merchant revenue, offtaker tops annual revenue up to the floor",
        price_name="floor premium",
        price=premium,
        priced=premium_eur_per_mw_year is None,
        direction="owner_pays",
        break_even=break_even,
        offtaker_res=off_res,
        owner_res=own_res,
        offtaker_cash=provider_flow + premium,
        owner_cash=owner_with_floor - premium,
        owner_benchmark=revenue,
        owner_benchmark_label="merchant without floor",
        merchant=revenue,
        offtaker=off,
        owner=own,
        scenarios=scenarios,
        series=name,
        deal_mw=deal_mw,
        mid_year=mid_year,
        terms={
            "floor_eur_per_mw_year": floor,
            "premium_eur_per_mw_year": premium,
            "upside_share": upside_share,
            "upside_threshold_eur_per_mw_year": threshold,
        },
        extras={
            "prob_floor_triggered_year": float(np.mean(revenue < floor)),
            "expected_floor_payout_eur_per_mw_year": float(put.mean()),
            "expected_upside_paid_eur_per_mw_year": float((upside_share * call).mean()),
            "max_floor_payout_eur_per_mw_year": float(put.max()),
        },
    )


def _bisect(func: Callable[[float], float], low: float, high: float, *, tol: float, max_iter: int = 200) -> float:
    f_low = func(low)
    for _ in range(max_iter):
        mid = 0.5 * (low + high)
        f_mid = func(mid)
        if (f_mid > 0.0) == (f_low > 0.0):
            low, f_low = mid, f_mid
        else:
            high = mid
        if high - low < tol:
            break
    return 0.5 * (low + high)


def solve_floor_upside_share(
    scenarios: ScenarioSet,
    *,
    floor_eur_per_mw_year: float,
    offtaker: RiskProfile | None = None,
    upside_threshold_eur_per_mw_year: float | None = None,
    target_premium_eur_per_mw_year: float = 0.0,
    deal_mw: float = 1.0,
    series: str | None = None,
    mid_year: bool = True,
    tol: float = 1e-7,
) -> float | None:
    """Upside share ``b`` that makes the provider's fair premium equal the target (0 = zero-premium collar).

    Returns ``None`` when even ``b = 1`` does not pay for the floor.
    """

    off = offtaker or default_offtaker()
    revenue = scenarios.get(series)
    floor = float(floor_eur_per_mw_year)
    threshold = floor if upside_threshold_eur_per_mw_year is None else float(upside_threshold_eur_per_mw_year)
    factors = discount_factors(scenarios.n_years, off.discount_rate, mid_year=mid_year)
    annuity = float(factors.sum())
    put_pv = np.maximum(floor - revenue, 0.0) @ factors
    call_pv = np.maximum(revenue - threshold, 0.0) @ factors
    margin_pv = off.required_margin_eur_per_mw_year * annuity

    def excess(share: float) -> float:
        # same formula as the offtaker reservation in price_floor, evaluated on precomputed legs
        reservation = (margin_pv - certainty_equivalent(share * call_pv - put_pv, off, scale=deal_mw)) / annuity
        return reservation - target_premium_eur_per_mw_year

    if excess(0.0) <= 0.0:
        return 0.0
    if excess(1.0) > 0.0:
        return None
    return _bisect(excess, 0.0, 1.0, tol=tol)


def price_partial_toll(
    scenarios: ScenarioSet,
    *,
    fixed_fraction: float,
    owner_merchant_share: float,
    toll_eur_per_mw_year: float | None = None,
    escalation_per_year: float = 0.0,
    offtaker: RiskProfile | None = None,
    owner: RiskProfile | None = None,
    deal_mw: float = 1.0,
    owner_capture: float = 1.0,
    series: str | None = None,
    mid_year: bool = True,
) -> ContractResult:
    """Partial toll: a share ``x`` of the capacity is tolled at ``K``; the rest is merchant with a revenue share.

    Owner cash: ``x K (1 + e)^y + (1 - x) s R_y``; offtaker cash: ``x (R_y - K (1 + e)^y) + (1 - x)(1 - s) R_y``,
    with ``s = owner_merchant_share``. ``K`` is quoted per MW of *tolled* capacity.
    """
    off = offtaker or default_offtaker()
    own = owner or default_owner()
    _check_deal(deal_mw, owner_capture)
    x, s = float(fixed_fraction), float(owner_merchant_share)
    if not 0.0 < x <= 1.0:
        raise ValueError("fixed_fraction must be in (0, 1]")
    if not 0.0 <= s <= 1.0:
        raise ValueError("owner_merchant_share must be in [0, 1]")
    name = series or scenarios.primary
    revenue = scenarios.get(name)
    n_years = scenarios.n_years
    growth = _escalation(n_years, escalation_per_year)
    d_off = discount_factors(n_years, off.discount_rate, mid_year=mid_year)
    d_own = discount_factors(n_years, own.discount_rate, mid_year=mid_year)
    g_off, g_own = float(d_off @ growth), float(d_own @ growth)
    offtaker_merchant = (x + (1.0 - x) * (1.0 - s)) * revenue
    owner_merchant_part = (1.0 - x) * s * revenue
    benchmark = owner_capture * revenue
    break_even = float((offtaker_merchant @ d_off).mean()) / (x * g_off)
    off_res = (
        certainty_equivalent(offtaker_merchant @ d_off, off, scale=deal_mw)
        - off.required_margin_eur_per_mw_year * float(d_off.sum())
    ) / (x * g_off)
    own_res = (
        certainty_equivalent(benchmark @ d_own, own, scale=deal_mw)
        - certainty_equivalent(owner_merchant_part @ d_own, own, scale=deal_mw)
    ) / (x * g_own)
    toll = off_res if toll_eur_per_mw_year is None else float(toll_eur_per_mw_year)
    fixed = toll * growth[None, :] * x
    return _result(
        contract="partial_toll",
        description=(
            f"Partial toll: {x:.0%} of capacity at a fixed toll, {1 - x:.0%} merchant with the owner keeping "
            f"{s:.0%} of that merchant revenue"
        ),
        price_name="toll on the fixed share",
        price=toll,
        priced=toll_eur_per_mw_year is None,
        direction="offtaker_pays",
        break_even=break_even,
        offtaker_res=off_res,
        owner_res=own_res,
        offtaker_cash=offtaker_merchant - fixed,
        owner_cash=owner_merchant_part + fixed,
        owner_benchmark=benchmark,
        owner_benchmark_label=f"merchant ({owner_capture:.0%} capture of simulated revenue)",
        merchant=revenue,
        offtaker=off,
        owner=own,
        scenarios=scenarios,
        series=name,
        deal_mw=deal_mw,
        mid_year=mid_year,
        terms={
            "fixed_fraction": x,
            "owner_merchant_share": s,
            "toll_eur_per_mw_year": toll,
            "escalation_per_year": escalation_per_year,
            "owner_capture": owner_capture,
        },
        extras={
            "owner_fixed_share_of_expected_cash": float(fixed.mean() / (fixed + owner_merchant_part).mean()),
        },
    )


def price_revenue_swap(
    scenarios: ScenarioSet,
    *,
    notional_fraction: float = 1.0,
    fixed_eur_per_mw_year: float | None = None,
    reference_series: str | None = None,
    asset_series: str | None = None,
    offtaker: RiskProfile | None = None,
    owner: RiskProfile | None = None,
    deal_mw: float = 1.0,
    owner_capture: float = 1.0,
    mid_year: bool = True,
) -> ContractResult:
    """Revenue swap: the owner keeps operating and swaps a reference revenue index for a fixed amount.

    Per MW-year: offtaker cash ``n (Ref_y - S)``; owner cash ``c Asset_y - n Ref_y + n S``. With the reference equal
    to the asset's own revenue this is a financially settled toll; with a separate index (e.g. a benchmark
    strategy simulated jointly) the owner keeps the basis ``c Asset - n Ref``.
    """
    off = offtaker or default_offtaker()
    own = owner or default_owner()
    _check_deal(deal_mw, owner_capture)
    n = float(notional_fraction)
    if not 0.0 < n <= 1.5:
        raise ValueError("notional_fraction must be in (0, 1.5]")
    ref_name = reference_series or scenarios.primary
    asset_name = asset_series or scenarios.primary
    reference = scenarios.get(ref_name)
    asset = scenarios.get(asset_name)
    n_years = scenarios.n_years
    d_off = discount_factors(n_years, off.discount_rate, mid_year=mid_year)
    d_own = discount_factors(n_years, own.discount_rate, mid_year=mid_year)
    a_off, a_own = float(d_off.sum()), float(d_own.sum())
    floating = n * reference
    unhedged = owner_capture * asset
    hedged_float = unhedged - floating
    break_even = float((floating @ d_off).mean()) / (n * a_off)
    off_res = (
        certainty_equivalent(floating @ d_off, off, scale=deal_mw) - off.required_margin_eur_per_mw_year * a_off
    ) / (n * a_off)
    own_res = (
        certainty_equivalent(unhedged @ d_own, own, scale=deal_mw)
        - certainty_equivalent(hedged_float @ d_own, own, scale=deal_mw)
    ) / (n * a_own)
    fixed = off_res if fixed_eur_per_mw_year is None else float(fixed_eur_per_mw_year)
    owner_cash = hedged_float + n * fixed
    pv_unhedged = unhedged @ d_own
    pv_hedged = owner_cash @ d_own
    var_unhedged = float(np.var(pv_unhedged))
    effectiveness = 1.0 - float(np.var(pv_hedged)) / var_unhedged if var_unhedged > 0.0 else float("nan")
    corr = float(np.corrcoef(reference.ravel(), asset.ravel())[0, 1]) if ref_name != asset_name else 1.0
    return _result(
        contract="swap",
        description=(
            f"Revenue swap on {n:.0%} notional: owner pays the `{ref_name}` revenue index and receives a fixed amount"
        ),
        price_name="fixed leg",
        price=fixed,
        priced=fixed_eur_per_mw_year is None,
        direction="offtaker_pays",
        break_even=break_even,
        offtaker_res=off_res,
        owner_res=own_res,
        offtaker_cash=floating - n * fixed,
        owner_cash=owner_cash,
        owner_benchmark=unhedged,
        owner_benchmark_label=f"merchant ({owner_capture:.0%} capture of `{asset_name}`)",
        merchant=asset,
        offtaker=off,
        owner=own,
        scenarios=scenarios,
        series=asset_name,
        deal_mw=deal_mw,
        mid_year=mid_year,
        terms={
            "notional_fraction": n,
            "fixed_eur_per_mw_year": fixed,
            "reference_series": ref_name,
            "asset_series": asset_name,
            "owner_capture": owner_capture,
        },
        extras={
            "hedge_effectiveness_pv_variance": effectiveness,
            "annual_correlation_reference_asset": corr,
            "basis_std_eur_per_mw_year": float(np.std(owner_capture * asset - reference)),
        },
        notes=("price is the fixed leg per MW of notional",),
    )
