"""Impact operators: :class:`~euroflex_bess_lab.realtime.settlement.ImpactModel` implementations built from data.

``CurrentFormulaImpact`` and ``NewFormula2026Impact`` hold a quarter-hour state (from a :class:`MarketData`
period) plus fitted response curves, and answer two questions:

- ``price_at(q)``: the counterfactual settlement price of every quarter-hour for a fleet deviation ``q``;
- ``adjust(qh_index, base_price, fleet_delta_mw)``: the :class:`ImpactModel` hook used by
  :func:`~euroflex_bess_lab.realtime.simulate` (``impact=..., fleet_mw=...``). It returns
  ``base_price + price_at(q) - price_at(0)``, so a zero deviation leaves prices bit-for-bit unchanged and the
  published price is kept wherever the state lacks a component (no information -> no impact).

Fitting helpers estimate the response curves (fixed-effect monotone fits, see :mod:`.curves`) and the alpha model
from a training period; they are deliberately separate from the state so that curves fitted on one period can be
applied to another (out-of-sample validation, or a policy that only knows history).
"""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field, replace
from typing import Any

import numpy as np
import pandas as pd
from numpy.typing import NDArray

from ..replay import MarketData
from .alpha import AlphaFit, AlphaModel, fit_alpha_model
from .curves import (
    DEFAULT_REGIME_EDGES_HOURS,
    RegimeCurves,
    ResponseCurve,
    fit_regime_curves,
    hour_groups,
    local_hour_regime,
)
from .response import (
    BidCurveDelta,
    CurveDelta,
    NewFormulaCurves,
    VolumeDelta,
    current_price,
    new_formula_price,
)
from .state import (
    CurrentState,
    NewFormulaState,
    current_state_from_market,
    new_state_from_market,
)

FloatArray = NDArray[np.float64]
IntArray = NDArray[np.int64]


# --------------------------------------------------------------------------------------------------------------
# fitted response bundles
# --------------------------------------------------------------------------------------------------------------


@dataclass(frozen=True)
class CurrentResponseFit:
    """Fitted pieces of the current-formula operator."""

    mip: RegimeCurves
    mdp: RegimeCurves
    alpha: AlphaModel
    regime_edges_hours: tuple[int, ...] = DEFAULT_REGIME_EDGES_HOURS
    pass_through: float = 1.0
    alpha_mode: str = "exact"
    meta: dict[str, Any] = field(default_factory=dict)

    def scaled(self, factor: float) -> CurrentResponseFit:
        """Volume response multiplied by ``factor`` (alpha unchanged): a misspecified-slope stress test."""
        return replace(self, mip=self.mip.scaled(factor), mdp=self.mdp.scaled(factor))

    def to_dict(self) -> dict[str, Any]:
        return {
            "mechanism": "current",
            "mip": self.mip.to_dict(),
            "mdp": self.mdp.to_dict(),
            "alpha": self.alpha.to_dict(),
            "regime_edges_hours": list(self.regime_edges_hours),
            "pass_through": self.pass_through,
            "alpha_mode": self.alpha_mode,
            "meta": self.meta,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> CurrentResponseFit:
        return cls(
            mip=RegimeCurves.from_dict(data["mip"]),
            mdp=RegimeCurves.from_dict(data["mdp"]),
            alpha=AlphaModel.from_dict(data["alpha"]),
            regime_edges_hours=tuple(int(x) for x in data.get("regime_edges_hours", DEFAULT_REGIME_EDGES_HOURS)),
            pass_through=float(data.get("pass_through", 1.0)),
            alpha_mode=str(data.get("alpha_mode", "exact")),
            meta=dict(data.get("meta", {})),
        )

    @classmethod
    def linear(
        cls, up_slope: float, down_slope: float, alpha: AlphaModel | None = None, **kwargs: Any
    ) -> CurrentResponseFit:
        """Transparent linear response (EUR/MWh per MW) for tests and stress scenarios."""
        return cls(
            mip=RegimeCurves(pooled=ResponseCurve.linear(abs(up_slope), "mip")),
            mdp=RegimeCurves(pooled=ResponseCurve.linear(-abs(down_slope), "mdp")),
            alpha=alpha or AlphaModel(),
            **kwargs,
        )


@dataclass(frozen=True)
class NewFormulaResponseFit:
    """Fitted product-price curves of the 2026 formula."""

    afrr_up: RegimeCurves
    afrr_down: RegimeCurves
    mfrr_up: RegimeCurves
    mfrr_down: RegimeCurves
    regime_edges_hours: tuple[int, ...] = DEFAULT_REGIME_EDGES_HOURS
    pass_through: float = 1.0
    displacement: str = "pro_rata"
    meta: dict[str, Any] = field(default_factory=dict)

    def scaled(self, factor: float) -> NewFormulaResponseFit:
        return replace(
            self,
            afrr_up=self.afrr_up.scaled(factor),
            afrr_down=self.afrr_down.scaled(factor),
            mfrr_up=self.mfrr_up.scaled(factor),
            mfrr_down=self.mfrr_down.scaled(factor),
        )

    def curves(self) -> NewFormulaCurves:
        return NewFormulaCurves(
            afrr_up=CurveDelta(self.afrr_up),
            afrr_down=CurveDelta(self.afrr_down),
            mfrr_up=CurveDelta(self.mfrr_up),
            mfrr_down=CurveDelta(self.mfrr_down),
        )

    def to_dict(self) -> dict[str, Any]:
        return {
            "mechanism": "new2026",
            "afrr_up": self.afrr_up.to_dict(),
            "afrr_down": self.afrr_down.to_dict(),
            "mfrr_up": self.mfrr_up.to_dict(),
            "mfrr_down": self.mfrr_down.to_dict(),
            "regime_edges_hours": list(self.regime_edges_hours),
            "pass_through": self.pass_through,
            "displacement": self.displacement,
            "meta": self.meta,
        }

    @classmethod
    def from_dict(cls, data: Mapping[str, Any]) -> NewFormulaResponseFit:
        return cls(
            afrr_up=RegimeCurves.from_dict(data["afrr_up"]),
            afrr_down=RegimeCurves.from_dict(data["afrr_down"]),
            mfrr_up=RegimeCurves.from_dict(data["mfrr_up"]),
            mfrr_down=RegimeCurves.from_dict(data["mfrr_down"]),
            regime_edges_hours=tuple(int(x) for x in data.get("regime_edges_hours", DEFAULT_REGIME_EDGES_HOURS)),
            pass_through=float(data.get("pass_through", 1.0)),
            displacement=str(data.get("displacement", "pro_rata")),
            meta=dict(data.get("meta", {})),
        )

    @classmethod
    def linear(cls, up_slope: float, down_slope: float, **kwargs: Any) -> NewFormulaResponseFit:
        up = RegimeCurves(pooled=ResponseCurve.linear(abs(up_slope), "up"))
        down = RegimeCurves(pooled=ResponseCurve.linear(-abs(down_slope), "down"))
        return cls(afrr_up=up, afrr_down=down, mfrr_up=up, mfrr_down=down, **kwargs)


# --------------------------------------------------------------------------------------------------------------
# fitting
# --------------------------------------------------------------------------------------------------------------


def estimate_pass_through(
    si: FloatArray,
    up: FloatArray,
    down: FloatArray,
    times: pd.DatetimeIndex,
    *,
    bounds: tuple[float, float] = (0.0, 1.0),
    winsor: tuple[float, float] = (0.01, 0.99),
) -> float:
    """Share of an SI change that Elia's aFRR + mFRR activation offsets: ``-d(U - D) / dSI`` between consecutive
    quarter-hours (OLS slope on winsorised changes, clipped to ``bounds``)."""
    idx = pd.DatetimeIndex(times)
    if len(idx) < 3:
        return 1.0
    step_ok = np.asarray((idx[1:] - idx[:-1]) == pd.Timedelta(minutes=15))
    k = np.flatnonzero(step_ok) + 1
    net = np.asarray(up, dtype=float) - np.asarray(down, dtype=float)
    dsi = np.asarray(si, dtype=float)[k] - np.asarray(si, dtype=float)[k - 1]
    dnet = net[k] - net[k - 1]
    ok = np.isfinite(dsi) & np.isfinite(dnet)
    if ok.sum() < 50:
        return 1.0
    x, y = dsi[ok], dnet[ok]
    x = np.clip(x, *np.quantile(x, winsor))
    y = np.clip(y, *np.quantile(y, winsor))
    x, y = x - x.mean(), y - y.mean()
    slope = float(np.dot(x, y) / np.dot(x, x)) if np.dot(x, x) > 0 else -1.0
    return float(np.clip(-slope, *bounds))


def _period_meta(times: pd.DatetimeIndex) -> dict[str, Any]:
    if len(times) == 0:
        return {}
    return {"train_start_utc": str(times[0]), "train_end_utc": str(times[-1] + pd.Timedelta(minutes=15))}


def fit_current_response(
    market: MarketData,
    *,
    regime_edges_hours: Sequence[int] = DEFAULT_REGIME_EDGES_HOURS,
    use_regimes: bool = True,
    n_knots: int = 10,
    min_regime_obs: int = 3000,
    alpha_test_fraction: float = 0.3,
    pass_through: float | None = None,
) -> tuple[CurrentResponseFit, AlphaFit]:
    """Fit MIP-vs-upward-volume and MDP-vs-downward-volume curves (within-hour fixed effects), the alpha model and the
    activation pass-through (estimated unless ``pass_through`` is given)."""
    state = current_state_from_market(market, regime_edges_hours=regime_edges_hours)
    groups = hour_groups(market.qh_times)
    regime = state.regime if use_regimes else None
    mip = fit_regime_curves(
        state.up_mw,
        state.mip_eur_mwh,
        increasing=True,
        regime=regime,
        groups=groups,
        n_knots=n_knots,
        min_regime_obs=min_regime_obs,
        label="mip_vs_up",
    )
    mdp = fit_regime_curves(
        state.down_mw,
        state.mdp_eur_mwh,
        increasing=False,
        regime=regime,
        groups=groups,
        n_knots=n_knots,
        min_regime_obs=min_regime_obs,
        label="mdp_vs_down",
    )
    alpha_fit = fit_alpha_model(
        state.si_mw,
        state.si_prev_mw,
        state.alpha_eur_mwh,
        mip=state.mip_eur_mwh,
        mdp=state.mdp_eur_mwh,
        test_fraction=alpha_test_fraction,
    )
    kappa = (
        estimate_pass_through(state.si_mw, state.up_mw, state.down_mw, market.qh_times)
        if pass_through is None
        else float(pass_through)
    )
    fit = CurrentResponseFit(
        mip=mip,
        mdp=mdp,
        alpha=alpha_fit.model,
        regime_edges_hours=tuple(regime_edges_hours),
        pass_through=kappa,
        alpha_mode=alpha_fit.mode,
        meta={**_period_meta(market.qh_times), "n_qh": market.n_qh, "use_regimes": use_regimes},
    )
    return fit, alpha_fit


def fit_new_formula_response(
    market: MarketData,
    *,
    regime_edges_hours: Sequence[int] = DEFAULT_REGIME_EDGES_HOURS,
    use_regimes: bool = True,
    n_knots: int = 10,
    min_regime_obs: int = 3000,
    min_mfrr_obs: int = 300,
    pass_through: float | None = None,
) -> NewFormulaResponseFit:
    """Fit aFRR / mFRR product-price curves of the 2026 formula (ODS212) with within-hour fixed effects.

    mFRR curves use day fixed effects, fewer knots and stronger winsorising (mFRR is activated in a few percent of
    quarter-hours and its down prices include -15,000 EUR/MWh outliers); with fewer than ``min_mfrr_obs`` activated
    quarter-hours the mFRR price is held fixed (flat curve).
    """
    state = new_state_from_market(market, regime_edges_hours=regime_edges_hours)
    groups = hour_groups(market.qh_times)
    regime = state.regime if use_regimes else None
    common: dict[str, Any] = {"groups": groups, "n_knots": n_knots, "min_regime_obs": min_regime_obs}
    afrr_up = fit_regime_curves(
        state.afrr_up_mw, state.afrr_price_up_eur_mwh, increasing=True, regime=regime, label="afrr_up", **common
    )
    afrr_down = fit_regime_curves(
        state.afrr_down_mw,
        state.afrr_price_down_eur_mwh,
        increasing=False,
        regime=regime,
        label="afrr_down",
        **common,
    )
    days = pd.DatetimeIndex(market.qh_times).tz_convert(market.timezone).normalize().asi8

    def mfrr_curve(volume: FloatArray, price: FloatArray, increasing: bool, label: str) -> RegimeCurves:
        active = (volume > 0.0) & np.isfinite(price)
        if int(active.sum()) < min_mfrr_obs:
            return RegimeCurves(pooled=ResponseCurve.flat(label=label, increasing=increasing))
        return fit_regime_curves(
            volume[active],
            price[active],
            increasing=increasing,
            regime=None,
            groups=days[active],
            n_knots=5,
            estimator="within",
            winsor=(0.05, 0.95),
            price_clip=(0.02, 0.98),
            label=label,
        )

    mfrr_up = mfrr_curve(state.mfrr_up_mw, state.mfrr_price_up_eur_mwh, True, "mfrr_up")
    mfrr_down = mfrr_curve(state.mfrr_down_mw, state.mfrr_price_down_eur_mwh, False, "mfrr_down")
    kappa = (
        estimate_pass_through(
            state.si_mw,
            state.afrr_up_mw + state.mfrr_up_mw,
            state.afrr_down_mw + state.mfrr_down_mw,
            market.qh_times,
        )
        if pass_through is None
        else float(pass_through)
    )
    return NewFormulaResponseFit(
        afrr_up=afrr_up,
        afrr_down=afrr_down,
        mfrr_up=mfrr_up,
        mfrr_down=mfrr_down,
        regime_edges_hours=tuple(regime_edges_hours),
        pass_through=kappa,
        meta={**_period_meta(market.qh_times), "n_qh": market.n_qh, "use_regimes": use_regimes},
    )


# --------------------------------------------------------------------------------------------------------------
# ImpactModel implementations
# --------------------------------------------------------------------------------------------------------------


def _previous_deviation(qh_index: IntArray, fleet_delta: FloatArray) -> FloatArray:
    """Deviation of quarter-hour ``k - 1`` for every entry (0 where ``k - 1`` is not in the arrays)."""
    order = np.argsort(qh_index, kind="stable")
    sorted_idx = qh_index[order]
    target = qh_index - 1
    pos = np.searchsorted(sorted_idx, target)
    pos_c = np.clip(pos, 0, max(len(sorted_idx) - 1, 0))
    found = (pos < len(sorted_idx)) & (sorted_idx[pos_c] == target) if len(sorted_idx) else np.zeros(0, dtype=bool)
    prev = np.zeros(len(qh_index), dtype=float)
    prev[found] = fleet_delta[order[pos_c[found]]]
    return prev


def _incremental(base: FloatArray, with_q: FloatArray, at_zero: FloatArray) -> FloatArray:
    delta = with_q - at_zero
    out: FloatArray = np.where(np.isfinite(delta), base + delta, base)
    return out


def _times_positions(source: pd.DatetimeIndex, target: pd.DatetimeIndex) -> IntArray:
    pos = source.get_indexer(target)
    if np.any(pos < 0):
        missing = target[pos < 0]
        raise ValueError(f"operator does not cover {len(missing)} quarter-hours (first: {missing[0]})")
    return np.asarray(pos, dtype=np.int64)


@dataclass
class CurrentFormulaImpact:
    """Endogenous price impact under the current Elia formula (``MIP + alpha`` / ``MDP - alpha``)."""

    state: CurrentState
    times: pd.DatetimeIndex
    fit: CurrentResponseFit
    up_delta: VolumeDelta | None = None
    down_delta: VolumeDelta | None = None
    name: str = "current_formula_impact"

    def __post_init__(self) -> None:
        if self.up_delta is None:
            self.up_delta = CurveDelta(self.fit.mip)
        if self.down_delta is None:
            self.down_delta = CurveDelta(self.fit.mdp)

    @classmethod
    def from_market(
        cls,
        market: MarketData,
        fit: CurrentResponseFit | None = None,
        *,
        bid_curves: tuple[FloatArray, FloatArray, FloatArray] | None = None,
        **kwargs: Any,
    ) -> CurrentFormulaImpact:
        """Operator over ``market``'s quarter-hours; ``fit`` defaults to curves fitted on ``market`` itself.

        ``bid_curves = (grid_mw, up_prices, down_prices)`` (rows aligned with ``market.qh_times``) replaces the fitted
        volume response by quarter-hour merit-order curves where they exist.
        """
        if fit is None:
            fit, _ = fit_current_response(market)
        state = current_state_from_market(market, regime_edges_hours=fit.regime_edges_hours)
        up: VolumeDelta = CurveDelta(fit.mip)
        down: VolumeDelta = CurveDelta(fit.mdp)
        if bid_curves is not None:
            grid, up_prices, down_prices = bid_curves
            up = BidCurveDelta(grid, up_prices, fallback=up, increasing=True)
            down = BidCurveDelta(grid, down_prices, fallback=down, increasing=False)
        return cls(state=state, times=market.qh_times, fit=fit, up_delta=up, down_delta=down, **kwargs)

    def __len__(self) -> int:
        return len(self.state)

    def _deltas(self) -> tuple[VolumeDelta, VolumeDelta]:
        assert self.up_delta is not None and self.down_delta is not None
        return self.up_delta, self.down_delta

    def price_at(
        self,
        q: float | FloatArray,
        *,
        rows: IntArray | None = None,
        q_prev: float | FloatArray | None = None,
    ) -> FloatArray:
        """Counterfactual price per quarter-hour (``q`` shape ``(n,)``, ``(n, k)`` or scalar)."""
        state = self.state if rows is None else self.state.take(np.asarray(rows, dtype=np.int64))
        up, down = self._deltas()
        n = len(state)
        qq = np.asarray(q, dtype=float)
        if qq.ndim == 0:
            qq = np.full(n, float(qq))
        qp = None if q_prev is None else np.broadcast_to(np.asarray(q_prev, dtype=float), (n,))
        return current_price(
            state,
            qq,
            up=up,
            down=down,
            alpha_model=self.fit.alpha,
            q_prev=qp,
            alpha_mode=self.fit.alpha_mode,
            pass_through=self.fit.pass_through,
        )

    def reconstruct(self) -> FloatArray:
        """Model price at zero deviation (equals the published price where all components are present)."""
        return self.price_at(0.0)

    def adjust(self, qh_index: IntArray, base_price: FloatArray, fleet_delta_mw: FloatArray) -> FloatArray:
        idx = np.asarray(qh_index, dtype=np.int64)
        q = np.asarray(fleet_delta_mw, dtype=float)
        base = np.asarray(base_price, dtype=float)
        q_prev = _previous_deviation(idx, q)
        with_q = self.price_at(q, rows=idx, q_prev=q_prev)
        at_zero = self.price_at(np.zeros(len(idx)), rows=idx, q_prev=np.zeros(len(idx)))
        return _incremental(base, with_q, at_zero)

    def aligned(self, market: MarketData) -> CurrentFormulaImpact:
        """Same operator restricted / re-ordered to ``market.qh_times`` (e.g. a simulation slice)."""
        pos = _times_positions(self.times, market.qh_times)
        state = self.state.take(pos)
        state = replace(state, curve_row=self.state.curve_row[pos])
        return replace(self, state=state, times=market.qh_times)


@dataclass
class NewFormula2026Impact:
    """Endogenous price impact under the planned 2026 Elia formula (volume-weighted aFRR / mFRR main components)."""

    state: NewFormulaState
    times: pd.DatetimeIndex
    fit: NewFormulaResponseFit
    name: str = "new2026_formula_impact"

    @classmethod
    def from_market(
        cls, market: MarketData, fit: NewFormulaResponseFit | None = None, **kwargs: Any
    ) -> NewFormula2026Impact:
        if fit is None:
            fit = fit_new_formula_response(market)
        state = new_state_from_market(market, regime_edges_hours=fit.regime_edges_hours)
        return cls(state=state, times=market.qh_times, fit=fit, **kwargs)

    def __len__(self) -> int:
        return len(self.state)

    def price_at(
        self,
        q: float | FloatArray,
        *,
        rows: IntArray | None = None,
        q_prev: float | FloatArray | None = None,
    ) -> FloatArray:
        """Counterfactual price per quarter-hour. ``q_prev`` is accepted for API symmetry (the 2026 formula has no
        dependence on the previous quarter-hour)."""
        del q_prev
        state = self.state if rows is None else self.state.take(np.asarray(rows, dtype=np.int64))
        n = len(state)
        qq = np.asarray(q, dtype=float)
        if qq.ndim == 0:
            qq = np.full(n, float(qq))
        return new_formula_price(
            state,
            qq,
            curves=self.fit.curves(),
            pass_through=self.fit.pass_through,
            displacement=self.fit.displacement,
        )

    def reconstruct(self) -> FloatArray:
        return self.price_at(0.0)

    def adjust(self, qh_index: IntArray, base_price: FloatArray, fleet_delta_mw: FloatArray) -> FloatArray:
        idx = np.asarray(qh_index, dtype=np.int64)
        q = np.asarray(fleet_delta_mw, dtype=float)
        base = np.asarray(base_price, dtype=float)
        with_q = self.price_at(q, rows=idx)
        at_zero = self.price_at(np.zeros(len(idx)), rows=idx)
        return _incremental(base, with_q, at_zero)

    def aligned(self, market: MarketData) -> NewFormula2026Impact:
        pos = _times_positions(self.times, market.qh_times)
        return replace(self, state=self.state.take(pos), times=market.qh_times)


ImpactOperator = CurrentFormulaImpact | NewFormula2026Impact


def operator_for(
    mechanism: str,
    market: MarketData,
    fit: CurrentResponseFit | NewFormulaResponseFit | None = None,
    **kwargs: Any,
) -> ImpactOperator:
    """Build the impact operator of ``mechanism`` (``current`` or ``new2026``) over ``market``."""
    if mechanism == "current":
        if fit is not None and not isinstance(fit, CurrentResponseFit):
            raise TypeError("current mechanism needs a CurrentResponseFit")
        return CurrentFormulaImpact.from_market(market, fit, **kwargs)
    if mechanism == "new2026":
        if fit is not None and not isinstance(fit, NewFormulaResponseFit):
            raise TypeError("new2026 mechanism needs a NewFormulaResponseFit")
        return NewFormula2026Impact.from_market(market, fit, **kwargs)
    raise KeyError(f"no impact operator for mechanism `{mechanism}`; known: current, new2026")


def regime_for_times(
    times: pd.DatetimeIndex, fit: CurrentResponseFit | NewFormulaResponseFit, timezone: str
) -> IntArray:
    return local_hour_regime(times, timezone=timezone, edges_hours=fit.regime_edges_hours)
